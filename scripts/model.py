import os
import shutil
from pathlib import Path
import numpy as np
from .utils import *
from .domain import *

def check_administrator_privilege():
    """Return whether or not user can enter the admin mode"""
    username = os.popen("id -un").read().strip("\n")
    return username in ["raziq", "raziqraif", "lanzhao", "rcampbel"]

def get_user_globalecon_project_dirnames():
    "Return the list of AgMIP projects that the current user is in"
    groups = os.popen("groups").read().strip("\n").split(" ")
    project_groups = [group for group in groups if "pr-agmipglobalecon" in group]
    project_dirnames = [p_group[len("pr-") :] for p_group in project_groups]

    if len(project_dirnames) == 0:
        # NOTE: This is just to make developing on local environment easier
        project_dirnames = ["agmipglobaleconagclim50iv"]
    return project_dirnames


class Model:
    WORKINGDIR_PATH = Path(__name__).parent.parent / "workingdir"  # <PROJECT_DIR>/workingdir
    UPLOADDIR_PATH = WORKINGDIR_PATH / "uploads"
    DOWNLOADDIR_PATH = WORKINGDIR_PATH / "downloads"
    DATA_PROJ_ROOT = Path('/data/projects/')
    DATA_PROJ_SUBD = Path('files/')

    def __init__(self):

        # Base app states
        self.javascript_model = JSAppModel()  # - Do info injection into JS context
        self.application_mode = ApplicationMode.USER
        self.is_user_an_admin = check_administrator_privilege()
        self.current_user_page = UserPage.FILE_UPLOAD  # - current user mode page
        self.furthest_active_user_page = UserPage.FILE_UPLOAD  # - furthest/last active user mode page
        self.input_data_entity = InputDataEntity()  # - domain entity for input / uploaded data file
        self.input_data_diagnosis: InputDataDiagnosis = InputDataDiagnosis()  # - domain entity for input data diagnosis
        self.output_data_entity: OutputDataEntity = OutputDataEntity()  # - domain entity for output / processed data
        
        # File upload page's states
        self.INFOFILE_PATH = (  # - path of downloadeable info file
            self.WORKINGDIR_PATH / "AgMIP GlobalEcon Data Submission Info.zip"
        )
        self.USER_GLOBALECON_PROJECTS = [
            (dirname[len("agmipglobalecon"):], dirname) for dirname in get_user_globalecon_project_dirnames()
        ]  # - GlobalEcon projects user is part of
        self.uploadedfile_name = ""
        self.associated_project_dirnames = []  # - associated GlobalEcon projects for this submission

        # Data spec page states
        # States for page have multiple dependencies, changes to state may trigger changes to other states 
        # Only define 1 state as instance attrib here, define other states as properties later
        # Define states as props b/c changes made need to be relayed to domain model
        
        self.VALID_MODEL_NAMES = None  # - valid model names
        
        # Integrity checking page's states
        # - result of row checks
        self.nrows_w_struct_issue = 0  # - num rows w/structural issues
        self.nrows_w_ignored_scenario = 0  # - num rows w/ignored scenario
        self.nrows_duplicates = 0  # - num duplicate rows
        self.nrows_accepted = 0  # - num rows that passed row checks
        
        # - paths to downloadable row files
        self.STRUCTISSUEFILE_PATH = self.DOWNLOADDIR_PATH / "Rows With Structural Issue.csv"
        self.DUPLICATESFILE_PATH = self.DOWNLOADDIR_PATH / "Duplicate Records.csv"
        self.IGNOREDSCENARIOFILE_PATH = self.DOWNLOADDIR_PATH / "Records With An Ignored Scenario.csv"
        self.ACCEPTEDFILE_PATH = self.DOWNLOADDIR_PATH / "Accepted Records.csv"
        
        # - result of label/field checks
        self.bad_labels_overview_tbl = []
        self.unknown_labels_overview_tbl = []
        
        # - valid labels to fix unknown field (based on associated col)
        self.VALID_SCENARIOS = None
        self.VALID_REGIONS = None
        self.VALID_VARIABLES = None
        self.VALID_ITEMS = None
        self.VALID_UNITS = None
        
        # Plausibility checking page's states
        self.outputfile_path = Path()  # - path to cleaned & processed file
        self.overridden_labels = 0
        self.active_visualization_tab = VisualizationTab.VALUE_TRENDS
        
        # - uploaded labels
        self.uploaded_scenarios = []
        self.uploaded_regions = []
        self.uploaded_items = []
        self.uploaded_variables = []
        self.uploaded_units = []
        self.uploaded_years = []

        # TODO Instead of exposing grouped data frame to View, create plot in domain layer under OutputDataEntity, display in view

        # - states for value trends visualization
        self.valuetrends_scenario = ""
        self.valuetrends_region = ""
        self.valuetrends_variable = ""
        self.valuetrends_table = None
        self.valuetrends_table_year_colname = ""
        self.valuetrends_table_value_colname = ""

        # - states for growth trends visualization
        self.growthtrends_scenario = ""
        self.growthtrends_region = ""
        self.growthtrends_variable = ""
        self.growthtrends_table = None
        self.growthtrends_table_year_colname = ""
        self.growthtrends_table_value_colname = ""

    def intro(self, view, controller):  
        """Introduce MVC modules to each other."""
        self.view = view
        self.controller = controller

    # Admin page methods

    def get_submitted_files_info(self):
        """Return list of submitted files info."""
        dirnames = os.popen(f'ls {self.DATA_PROJ_ROOT}').read().split()
        project_dirnames = [dirname for dirname in dirnames if dirname[:len("agmipglobalecon")] == "agmipglobalecon"]
        files_info = []

        for project_dirname in project_dirnames:
            submissiondir_path = self.DATA_PROJ_ROOT / project_dirname / self.DATA_PROJ_SUBD / ".submissions"
            accepted_files = os.popen(f'ls {submissiondir_path} | grep .csv').read().split()
            pending_files = os.popen(f'ls {submissiondir_path / ".pending"} | grep [0-9].csv').read().split()

            for filename in accepted_files:
                files_info.append([filename, project_dirname, "Accepted"])

            for filename in pending_files:
                files_info.append([filename, project_dirname, "Pending"])

        return files_info

    # File upload page methods

    def remove_uploaded_file(self):
        """Remove uploaded file from upload directory."""
        file_path = self.UPLOADDIR_PATH / Path(self.uploadedfile_name)
        file_path.unlink()

    # Data spec page methods

    def init_data_specification_page_states(self, file_name):
        """Init states in data spec pages (only after just become active)."""
        # Note page may become active / inactive mult times
        
        # Re-initialize all states
        try:
            self.input_data_entity = InputDataEntity.create(self.UPLOADDIR_PATH / file_name)
        except Exception as e:
            return str(e)
        
        valid_delimiters = Delimiter.get_models()

        # Guess info about input file
        self.input_data_entity.guess_delimiter(valid_delimiters)
        self.input_data_entity.guess_header_is_included()
        self.input_data_entity.guess_initial_lines_to_skip()
        self.input_data_entity.guess_model_name_n_column_assignments()

    def validate_data_specification_input(self):
        if len(self.input_data_entity.model_name) == 0:
            return "Model name is empty"
        elif len(self.input_data_entity.delimiter) == 0:
            return "Delimiter is empty"
        elif int(self.input_data_entity.initial_lines_to_skip) < 0:
            return "Number of lines cannot be negative"
        elif len(self.assigned_scenario_column) == 0:
            return "Scenario column is empty"
        elif len(self.assigned_region_column) == 0:
            return "Region column is empty"
        elif len(self.assigned_variable_column) == 0:
            return "Variable column is empty"
        elif len(self.assigned_item_column) == 0:
            return "Item column is empty"
        elif len(self.assigned_unit_column) == 0:
            return "Unit column is empty"
        elif len(self.assigned_year_column) == 0:
            return "Year column is empty"
        elif len(self.assigned_value_column) == 0:
            return "Value column is empty"
        elif (  # Ensure no duplicate assignment, should have set of 7 cols
            len(
                set(
                    [
                        self.assigned_scenario_column,
                        self.assigned_region_column,
                        self.assigned_variable_column,
                        self.assigned_item_column,
                        self.assigned_unit_column,
                        self.assigned_year_column,
                        self.assigned_value_column,
                    ]
                )
            )
            < 7
        ):
            return "Output data has duplicate columns"

        return None

    # Integrity checking page methods

    def init_integrity_checking_page_states(self):
        # Diagnose input data
        self.input_data_diagnosis = InputDataDiagnosis.create(self.input_data_entity)
        
        # Map diagnosis results to page states
        self.nrows_w_struct_issue = self.input_data_diagnosis.nrows_w_struct_issue
        self.nrows_w_ignored_scenario = self.input_data_diagnosis.nrows_w_ignored_scenario
        self.nrows_accepted = self.input_data_diagnosis.nrows_accepted
        self.nrows_duplicates = self.input_data_diagnosis.nrows_duplicate
        self.bad_labels_overview_tbl = [
            [label_info.label, label_info.associated_column, label_info.fix]
            for label_info in self.input_data_diagnosis.bad_labels
        ]
        self.unknown_labels_overview_tbl = [
            [
                label_info.label,
                label_info.associated_column,
                label_info.closest_match,
                label_info.fix,
                label_info.override,
            ]
            for label_info in self.input_data_diagnosis.unknown_labels
        ]
        MIN_LABEL_OVERVIEW_TABLE_NROWS = 3
        self.bad_labels_overview_tbl += [["-", "-", "-"] for _ in range(MIN_LABEL_OVERVIEW_TABLE_NROWS)]
        self.unknown_labels_overview_tbl += [["-", "-", "-", "", False] for _ in range(MIN_LABEL_OVERVIEW_TABLE_NROWS)]

    def validate_unknown_labels_table(self, unknown_labels_table):

        for row in unknown_labels_table:
            _, _, _, fix, override = row

            if override == True and fix.strip() != "":
                return "Unknown labels cannot be both fixed and overridden"

    # Plausibility checking page methods

    def init_plausibility_checking_page_states(self, unknown_labels_table):
        popup_message = None
        # Pass unknown labels table back to input data diagnosis
        # NOTE Table contains fix or override actions selected by user
        # NOTE Must ignore dummy rows
        self.input_data_diagnosis.unknown_labels = [
            UnknownLabelInfo(
                label=str(row[0]),
                associated_column=str(row[1]),
                closest_match=str(row[2]),
                fix=str(row[3]),
                override=bool(row[4]),
            )
            for row in unknown_labels_table
        ]
        self.overridden_labels = len(
            [label_info for label_info in self.input_data_diagnosis.unknown_labels if label_info.override == True]
        )
        # Create output data based on info from input data & diagnosis
        self.output_data_entity = OutputDataEntity.create(self.input_data_entity, self.input_data_diagnosis)

        if self.output_data_entity:

            if self.input_data_diagnosis.rediagnose_n_filter_output_data(self.output_data_entity):
                self.output_data_entity = OutputDataEntity.create_from_rediagnosed_n_filtered_output_data(self.input_data_entity, self.input_data_diagnosis)
                popup_message = "After fixing some unknown variable or unit fields, the application found more records " \
                    "that contain out-of-bound values. The application has filtered out these records from the output data " \
                    "but it does not have a feature to report these records yet."

            # Map attribs from output data entity to page states
            self.outputfile_path = self.output_data_entity.file_path
            self.uploaded_scenarios = ["", *self.output_data_entity.unique_scenarios]
            self.uploaded_regions = ["", *self.output_data_entity.unique_regions]
            self.uploaded_variables = ["", *self.output_data_entity.unique_variables]
            self.uploaded_items = ["", *self.output_data_entity.unique_items]

            # Reset active tab
            self.active_visualization_tab = VisualizationTab.VALUE_TRENDS

            # Set default values if exist
            
            if "SSP2_NoMt_NoCC" in self.uploaded_scenarios:
                self.valuetrends_scenario = "SSP2_NoMt_NoCC"
                self.growthtrends_scenario = "SSP2_NoMt_NoCC"
            
            if "WLD" in self.uploaded_regions:
                self.valuetrends_region = "WLD"
                self.growthtrends_region = "WLD"
            
            if "PROD" in self.uploaded_variables:
                self.valuetrends_variable = "PROD"
                self.growthtrends_variable = "PROD"
            
            self.valuetrends_table = None
            self.growthtrends_table = None
        else:
            popup_message = 'ERROR: Unable to apply changes. Please adjust fixes or overrides.'

        return popup_message

    def update_valuetrends_visualization_states(self):
        self.valuetrends_table_value_colname = self.output_data_entity.VALUE_COLNAME
        self.valuetrends_table_year_colname = self.output_data_entity.YEAR_COLNAME
        self.valuetrends_table = self.output_data_entity.get_value_trends_table(
            self.valuetrends_scenario, self.valuetrends_region, self.valuetrends_variable
        )

    def update_growthtrends_visualization_states(self):
        self.growthtrends_table_value_colname = self.output_data_entity.VALUE_COLNAME
        self.growthtrends_table_year_colname = self.output_data_entity.YEAR_COLNAME
        self.growthtrends_table = self.output_data_entity.get_growth_trends_table(
            self.valuetrends_scenario, self.valuetrends_region, self.valuetrends_variable
        )

    def submit_processed_file(self):
        """Submit processed file to correct dir."""

        for project_dirname in self.associated_project_dirnames:
            outputfile_dstpath = (
                self.DATA_PROJ_ROOT / project_dirname / self.DATA_PROJ_SUBD / ".submissions" / ".pending" / self.outputfile_path.name

                if self.overridden_labels > 0
                else self.DATA_PROJ_ROOT / project_dirname / self.DATA_PROJ_SUBD / ".submissions" / self.outputfile_path.name
            )

            shutil.copy(self.outputfile_path, outputfile_dstpath)

            # Submitfile detailing override request or create new data cube
            if self.overridden_labels > 0:
                requestinfo_dstpath = outputfile_dstpath.parent / (outputfile_dstpath.stem + "_OverrideInfo.csv")

                with open(str(requestinfo_dstpath), "w+") as infofile:

                    for label_info in self.input_data_diagnosis.unknown_labels:

                        if label_info.override == True:
                            line = f"{label_info.label},{label_info.associated_column},{label_info.closest_match}\n"
                            infofile.write(line)
            else:
                submissiondir_path = outputfile_dstpath.parent
                submission_files_wildcard = str(submissiondir_path / "*.csv")
                # Print content of all csv files, remove duplicates, redirect output to merged.csv
                # TODO Ignore content of existing merged.csv when printing
                os.system(f"cat {submission_files_wildcard} | uniq > merged.csv")

    # Data spec page properties
    # NOTE See comment in constructor for reason for these props
    # NOTE Most getters can removed if allow View to read from domain layer directly
    #      Same for most setters if we allow Controller to write to domain layer directly 
    #      But exposing domain entity to View and Controller could create unwanted dependencies

    # - props for input format spec section

    @property
    def model_name(self):
        return self.input_data_entity.model_name

    @model_name.setter
    def model_name(self, value):
        self.input_data_entity.model_name = value

    @property
    def header_is_included(self):
        return self.input_data_entity.header_is_included

    @header_is_included.setter
    def header_is_included(self, value):
        self.input_data_entity.header_is_included = value

    @property
    def delimiter(self):
        return self.input_data_entity.delimiter

    @delimiter.setter
    def delimiter(self, value):
        self.input_data_entity.delimiter = value
        self.input_data_entity.guess_model_name_n_column_assignments()

    @property
    def lines_to_skip(self) -> int:
        return self.input_data_entity.initial_lines_to_skip

    @lines_to_skip.setter
    def lines_to_skip(self, value: int):
        self.input_data_entity.initial_lines_to_skip = value
        self.input_data_entity.guess_model_name_n_column_assignments()

    @property
    def scenarios_to_ignore_str(self):
        return "".join(self.input_data_entity.scenarios_to_ignore)

    @scenarios_to_ignore_str.setter
    def scenarios_to_ignore_str(self, value):
        value = value.strip()
        scenarios = value.split(",") if value != "" else []
        scenarios = [scenario.strip() for scenario in scenarios]
        self.input_data_entity.scenarios_to_ignore = scenarios

    # - props for col assign section

    @property
    def column_assignment_options(self):
        input_header = list(self.input_data_preview_content[0])  # Header / 1st row of input data preview
        return [] if "" in input_header else input_header  # NOTE Assumes empty str is only when header row empty

    @property
    def assigned_scenario_column(self):
        return ("", *self.column_assignment_options)[self.input_data_entity.scenario_colnum]

    @assigned_scenario_column.setter
    def assigned_scenario_column(self, value):
        self.input_data_entity.scenario_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_region_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.region_colnum]

    @assigned_region_column.setter
    def assigned_region_column(self, value):
        self.input_data_entity.region_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_variable_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.variable_colnum]

    @assigned_variable_column.setter
    def assigned_variable_column(self, value):
        self.input_data_entity.variable_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_item_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.item_colnum]

    @assigned_item_column.setter
    def assigned_item_column(self, value):
        self.input_data_entity.item_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_unit_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.unit_colnum]

    @assigned_unit_column.setter
    def assigned_unit_column(self, value):
        self.input_data_entity.unit_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_year_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.year_colnum]

    @assigned_year_column.setter
    def assigned_year_column(self, value):
        self.input_data_entity.year_colnum = ([""] + self.column_assignment_options).index(value)

    @property
    def assigned_value_column(self):
        return ([""] + self.column_assignment_options)[self.input_data_entity.value_colnum]

    @assigned_value_column.setter
    def assigned_value_column(self, value):
        self.input_data_entity.value_colnum = ([""] + self.column_assignment_options).index(value)

    # - properties for data preview sections

    @property
    def input_data_preview_content(self):
        """Return preview table content in an ndarray"""
        # Get constants
        NROWS = 3
        DEFAULT_CONTENT = np.array(["" for _ in range(3)]).reshape((NROWS, 1))
        preview_table = self.input_data_entity.sample_parsed_input_data[:NROWS]

        # Ensure enough num rows
        if len(preview_table) == 0:
            return DEFAULT_CONTENT
        elif len(preview_table) < 3:
            ncolumns = len(preview_table[0])
            empty_row = ["" for _ in range(ncolumns)]
            preview_table = (preview_table + [empty_row, empty_row])[:NROWS]  # add empty rows, trim excess rows

        # Prepare header row
        if self.input_data_entity.header_is_included:
            # Prepend header cells with a), b), c), d) ...
            A_ASCII = 97
            preview_table[0] = [
                chr(A_ASCII + col_idx) + ")  " + preview_table[0][col_idx] for col_idx in range(len(preview_table[0]))
            ]
        else:
            # Create header row
            ncolumns = len(preview_table[0])
            header = ["Column " + str(i + 1) for i in range(ncolumns)]
            preview_table = [header] + preview_table[: NROWS - 1]

        return np.array(preview_table)

    @property
    def output_data_preview_content(self):
        # Return preview table content in ndarray
        # Content built on top of input data preview content
        NROWS = 3
        # Lambda to return col content, given title & col num assignment
        get_column_content = (
            lambda title, assigned_colnum: [title] + ["" for _ in range(NROWS - 1)]
            if assigned_colnum == 0
            else [title] + [self.input_data_preview_content[row][assigned_colnum - 1] for row in range(1, NROWS)]
        )

        # Get content of all cols
        model_col = ["Model", self.input_data_entity.model_name, self.input_data_entity.model_name]
        scenario_col = get_column_content("Scenario", self.input_data_entity.scenario_colnum)
        region_col = get_column_content("Region", self.input_data_entity.region_colnum)
        variable_col = get_column_content("Variable", self.input_data_entity.variable_colnum)
        item_col = get_column_content("Item", self.input_data_entity.item_colnum)
        unit_col = get_column_content("Unit", self.input_data_entity.unit_colnum)
        year_col = get_column_content("Year", self.input_data_entity.year_colnum)
        value_col = get_column_content("Value", self.input_data_entity.value_colnum)
        
        return np.array(
            [model_col, scenario_col, region_col, variable_col, item_col, unit_col, year_col, value_col]
        ).transpose()

    def load_rules(self, proj_path):
        DataRuleRepository.load(self.DATA_PROJ_ROOT, proj_path, self.DATA_PROJ_SUBD)
        self.VALID_MODEL_NAMES = DataRuleRepository.query_model_names()
        self.VALID_SCENARIOS = DataRuleRepository.query_scenarios()
        self.VALID_REGIONS = DataRuleRepository.query_regions()
        self.VALID_VARIABLES = DataRuleRepository.query_variables()
        self.VALID_ITEMS = DataRuleRepository.query_items()
        self.VALID_UNITS = DataRuleRepository.query_units()
