import os
import math
import csv
import difflib
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd
from pandas import DataFrame

WORKINGDIR_PATH = Path(__name__).parent.parent / "workingdir"  # <PROJECT_DIR>/workingdir
DOWNLOADDIR_PATH = WORKINGDIR_PATH / "downloads"


class BadLabelInfo:
    """Store info about a bad label."""
    # NOTE "bad label": label/field that doesn't follow correct protocol but can be fixed automatically
    # NOTE __hash__() overridden! Not safe to use w/hashtable-based data struct!

    def __init__(self, label, associated_column, fix):
        self.label = label
        self.associated_column = associated_column
        self.fix = fix

    def __hash__(self):
        """Ensure two objs w/same attribute values produces same hash."""
        # NOTE: When attrib values change, hash value changes! 
        #       Be careful when using with hashtable-based data struct (e.g. dict, set)!
        return hash(self.label + self.associated_column + self.fix)

    def __eq__(self, obj):
        """ Override equality operator for convenience."""
        if not isinstance(obj, BadLabelInfo):
            return False

        return (self.label == obj.label) and (self.associated_column == obj.associated_column) and (self.fix == obj.fix)


class UnknownLabelInfo:
    """Store info about an unknown label."""
    # NOTE __hash__() overridden! Not safe to use w/hashtable-based data struct!

    def __init__(self, label, associated_column, closest_match, fix, override):
        self.label = label
        self.associated_column = associated_column
        self.closest_match = closest_match
        self.fix = fix
        self.override = override

    def __hash__(self):
        """ Override equality operator for convenience."""
        return hash(self.label + self.associated_column + self.closest_match + self.fix + str(self.override))

    def __eq__(self, obj):
        """ Override equality operator for convenience """
        if not isinstance(obj, UnknownLabelInfo):
            return False

        return (self.label == obj.label) and (self.associated_column == obj.associated_column) and (self.closest_match == obj.closest_match) and (self.fix == obj.fix) and (self.override == obj.override)

    def __str__(self):
        return f"{self.label},{self.associated_column},{self.closest_match},{self.fix},{self.override}"


class InputDataEntity:
    """Store input data file."""
    # Stores inforequired for parsing input DataFrame & transforming into correct arrangement
    # Provide utilities to guess and preview parsed input data

    _NROWS_IN_SAMPLE_DATA = 1000

    def __init__(self):

        # Input format spec attribs
        self.model_name = ""
        self.header_is_included = False
        self.scenarios_to_ignore = []
        self._file_nrows = 0
        
        # Input format spec attribs & file path only accessed via props
        self._file_path = Path()
        self._delimiter = ""
        self._initial_lines_to_skip = 0
        
        # Col assignment attribs
        self.scenario_colnum = 0   # colnum -> column number (1-based indexing)
        self.region_colnum = 0
        self.variable_colnum = 0
        self.item_colnum = 0
        self.unit_colnum = 0
        self.year_colnum = 0
        self.value_colnum = 0

        # Private attribs to help calc sample parsed input data
        # NOTE: Keep track of "topmost" sample & "nonskipped" sample to avoid loading entire file into mem
        #       Premature optimization, could've been simpler
        # TODO: Simplify operations that require these two sample attribs
        self._input_data_topmost_sample = []  # top X input data
        self._input_data_nonskipped_sample = []  # top X non-skipped input data
        self._sample_parsed_input_data_memo = None

    @classmethod
    def create(cls, file_path):
        """Return instance of this class"""

        entity = InputDataEntity()
        entity._file_path = file_path
        entity._input_data_topmost_sample = []
        entity._input_data_nonskipped_sample = []

        try:

            with open(str(file_path)) as csvfile:
                lines = csvfile.readlines()
                entity._file_nrows = len(lines)
                entity._input_data_topmost_sample = lines[: entity._NROWS_IN_SAMPLE_DATA]
                entity._input_data_nonskipped_sample = (
                    lines[entity.initial_lines_to_skip: entity.initial_lines_to_skip + entity._NROWS_IN_SAMPLE_DATA]
                    if entity.initial_lines_to_skip < entity._file_nrows
                    else []
                )
        except:
            raise Exception("Error when opening file")

        return entity

    def guess_delimiter(self, valid_delimiters):
        """Guess delimiter from sample input data, update value."""
        # Return True if the guess successful
        sample = "\n".join(self._input_data_topmost_sample)
        delimiters = "".join(valid_delimiters)
        format_sniffer = csv.Sniffer()

        try:
            csv_dialect = format_sniffer.sniff(sample, delimiters=delimiters)
        except csv.Error:  # Could be due to insufficient sample size, etc
            return False

        self.delimiter = csv_dialect.delimiter
        return True

    def guess_header_is_included(self):
        """Guess if a header row included in sample input data, update value."""
        # Return True if the guess successful
        sample = "\n".join(self._input_data_topmost_sample)
        format_sniffer = csv.Sniffer()

        try:
            self.header_is_included = format_sniffer.has_header(sample)
        except csv.Error:  # Could be due to insufficient sample size, etc
            return False

        return True

    def guess_initial_lines_to_skip(self):
        """Guess init number of lines to skip, update value."""
        # Return True if the guess successful
        rows = [
            row.split(self.delimiter) if len(self.delimiter) > 0 else [row] for row in self._input_data_topmost_sample
        ]

        if len(rows) == 0:
            return True

        # Use the most frequent no. of columns as a proxy for the no. of columns of a 'clean' row
        _ncolumns_bincount = np.bincount([len(row) for row in rows])
        most_frequent_ncolumns = int(_ncolumns_bincount.argmax())  # type-casted to raise error for non-int values

        # Skip initial lines w/mimatched num cols

        count = 0

        for row in rows:
            if len(row) == most_frequent_ncolumns:
                break
            count += 1

        # Assume guess failed if guessed number too much
        if count > self._NROWS_IN_SAMPLE_DATA * 0.9:
            self.initial_lines_to_skip = 0
            return False

        self.initial_lines_to_skip = count
        return True

    def guess_model_name_n_column_assignments(self):
        """Guess model name & col assignments, mutate appropariate states."""
        # Return True if some guesses successful
        sample_parsed_csv_rows = self.sample_parsed_input_data
        nrows = len(sample_parsed_csv_rows)
        ncols = len(sample_parsed_csv_rows[0]) if nrows > 0 else 0

        if nrows == 0 or ncols == 0:
            return False

        quotes = '\'\"`'
        guessed_something = False

        for col_index in range(ncols):

            for row_index in range(nrows):
                cell_value = sample_parsed_csv_rows[row_index][col_index].strip(quotes)
                successful_guess_id = self._guess_model_name_n_column_assignments_util(cell_value, col_index)

                if successful_guess_id != -1:
                    guessed_something = True
                    break

        return guessed_something

    def _guess_model_name_n_column_assignments_util(self, cell_value, col_index):
        """Return -1 if no guesses made, non-negative int if guess made."""
        # Each type of successful guess is associated with unique non-negative int
        
        if DataRuleRepository.query_label_in_model_names(cell_value):
            self.model_name = cell_value
            return 0
        elif (cell_value == "Scenario") or DataRuleRepository.query_label_in_scenarios(cell_value):
            self.scenario_colnum = col_index + 1
            return 1
        elif (cell_value == "Region") or DataRuleRepository.query_label_in_regions(cell_value):
            self.region_colnum = col_index + 1
            return 2
        elif (cell_value == "Variable") or DataRuleRepository.query_label_in_variables(cell_value):
            self.variable_colnum = col_index + 1
            return 3
        elif (cell_value == "Item") or DataRuleRepository.query_label_in_items(cell_value):
            self.item_colnum = col_index + 1
            return 4
        elif (cell_value == "Unit") or DataRuleRepository.query_label_in_units(cell_value):
            self.unit_colnum = col_index + 1
            return 5

        try:
            # Reminder(<float value in str repr>) will raise error
            if (1000 < int(cell_value)) and (int(cell_value) < 9999):
                self.year_colnum = col_index + 1
                return 6
        except ValueError:
            pass

        try:
            float(cell_value)
            self.value_colnum = col_index + 1
            return 7
        except ValueError:
            pass

        return -1

    @property
    def delimiter(self):
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value):
        self._delimiter = value
        # Reset col assignments & parsed input data sample
        self._sample_parsed_input_data_memo = None
        self.scenario_colnum = 0
        self.region_colnum = 0
        self.variable_colnum = 0
        self.item_colnum = 0
        self.unit_colnum = 0
        self.year_colnum = 0
        self.value_colnum = 0

    @property
    def file_path(self):
        return self._file_path

    @property
    def initial_lines_to_skip(self):
        return self._initial_lines_to_skip

    @initial_lines_to_skip.setter
    def initial_lines_to_skip(self, value):
        self._initial_lines_to_skip = value
        self._sample_parsed_input_data_memo = None

        # Reset column assignments if all file content was skipped
        if value > self._file_nrows:
            self.scenario_colnum = 0
            self.region_colnum = 0
            self.variable_colnum = 0
            self.item_colnum = 0
            self.unit_colnum = 0
            self.year_colnum = 0
            self.value_colnum = 0

        # Recompute input data sample
        if value == 0:
            self._input_data_nonskipped_sample = self._input_data_topmost_sample
            return

        self._input_data_nonskipped_sample = []

        try:

            with open(str(self.file_path)) as csvfile:
                lines = csvfile.readlines()
                self._input_data_topmost_sample = lines[: self._NROWS_IN_SAMPLE_DATA]
                self._input_data_nonskipped_sample = (
                    lines[self.initial_lines_to_skip: self.initial_lines_to_skip + self._NROWS_IN_SAMPLE_DATA]
                )
        except:
            return

    @property
    def sample_parsed_input_data(self):
        """Parse & process subset of raw input data, return result."""
        # NOTE: Processing not exhaustive, only includes:
        #        - Skipping initial rows
        #        - Splitting rows based on delimiter
        #        - Guessing correct num cols & removing rows wi/mismatched num cols
        # NOTE: This prop queried by mult dependents, value is memoized to avoid recalc
        #       Every time this property's dependencies (like delimiter) updated, memo must be reset!

        if self._sample_parsed_input_data_memo is not None:
            return self._sample_parsed_input_data_memo

        # Split rows in sample input data
        rows = [
            row.split(self.delimiter) if self.delimiter != "" else [row] for row in self._input_data_nonskipped_sample
        ]

        # Return if input data has no rows
        if len(rows) == 0:
            self._sample_parsed_input_data_memo = rows
            return rows

        # Use most frequent num cols in sample data as proxy for num cols in clean row
        # NOTE: If num dirty rows > num clean rows, will have incorrect result!
        _ncolumns_bincount: np.ndarray = np.bincount([len(row) for row in rows])
        most_frequent_ncolumns = int(_ncolumns_bincount.argmax())  # type-casted to raise error for non-int values
        # Prune rows w/mismatched cols
        rows = [row for row in rows if len(row) == most_frequent_ncolumns]
        self._sample_parsed_input_data_memo = rows
        return rows

    def __str__(self):
        return f"""
        > Input Data Entity
        File path = {str(self.file_path)}
        Model name = {self.model_name}
        Delimiter = {self.delimiter}
        Header is included = {self.header_is_included}
        Scenario colnum = {self.scenario_colnum}
        Region colnum = {self.region_colnum}
        Variable colnum = {self.variable_colnum}
        Item colnum = {self.item_colnum}
        Unit colnum = {self.unit_colnum}
        Year colnum = {self.year_colnum}
        Value colnum = {self.value_colnum}
        """


class InputDataDiagnosis:
    """Store input data diagnosis."""
    # Stores diagnosis results, provides diagnosis utility methods
    # TODO Consider abstracting some functionalities into Factory class & Service class

    _DOWNLOADDIR_PATH = WORKINGDIR_PATH / "downloads"
    # Col names used for reporting "associated columns" in bad labels table & unknown labels table
    SCENARIO_COLNAME = "Scenario"
    REGION_COLNAME = "Region"
    VARIABLE_COLNAME = "Variable"
    ITEM_COLNAME = "Item"
    UNIT_COLNAME = "Unit"
    YEAR_COLNAME = "Year"
    VALUE_COLNAME = "Value"
    # File destination paths for diagnosed rows
    STRUCTISSUEROWS_DSTPATH = _DOWNLOADDIR_PATH / "Rows With Structural Issue.csv"
    DUPLICATESROWS_DSTPATH = _DOWNLOADDIR_PATH / "Duplicate Records.csv"
    IGNOREDSCENARIOROWS_DSTPATH = _DOWNLOADDIR_PATH / "Records With An Ignored Scenario.csv"
    ACCEPTEDROWS_DSTPATH = _DOWNLOADDIR_PATH / "Accepted Records.csv"
    # File destination path for filtered output data
    FILTERED_OUTPUT_DSTPATH = _DOWNLOADDIR_PATH / "Filtered Output Data.csv"

    def __init__(self):
        # Results of row checks
        self.nrows_w_struct_issue = 0
        self.nrows_w_ignored_scenario = 0
        self.nrows_duplicate = 0
        self.nrows_accepted = 0

        # Results of field checks
        self.bad_labels = []  # Labels that violate data protocol but can be fixed automatically
        self.unknown_labels = []  # Labels that violate data protocol but cannot be fixed automatically
        
        # Valid years that do not exist in data protocol yet
        # Needs to be included into  data protocol file to generate appropriate GAMS header file later
        self.unknown_years = set() 
        
        # Private helper attribs
        
        # Info about input file
        self._input_entity = InputDataEntity()
        self._correct_ncolumns = 0
        self._largest_ncolumns = 0
        
        # Row occurrence dictionary for duplicate checking
        self._row_occurence_dict = {}

    def rediagnose_n_filter_output_data(self, output_entity): 
        """Re-diagnose & filter output data, store result in appropriate file.""" 
        # Returns whether new issues were found after the re-diagnosis
        # If unknown variables or units swapped w/valid label
        #   associated values never checked against acceptable range 
        # Must check & filter them here

        has_new_issues = False
        is_fixed_unknown_variable_or_unit = (
            lambda label_info: (label_info.associated_column == self.VARIABLE_COLNAME) or (label_info.associated_column == self.UNIT_COLNAME) and (label_info.fix != "")
        )
        fixed_variables_or_units = set(label_info.fix for label_info in self.unknown_labels if is_fixed_unknown_variable_or_unit(label_info))
        # TODO: Reimplement using vectorization techniques for better performance
        # TODO: Update files / attributes relating to accepted rows & rows w/structural issue

        with open(output_entity.file_path, "r") as outputfile, open(self.FILTERED_OUTPUT_DSTPATH, "w+") as filteredoutputfile:
            rows = outputfile.readlines()
            variable_colidx = output_entity.processed_data.columns.tolist().index(output_entity.VARIABLE_COLNAME)
            value_colidx = output_entity.processed_data.columns.tolist().index(output_entity.VALUE_COLNAME)
            unit_colidx = output_entity.processed_data.columns.tolist().index(output_entity.UNIT_COLNAME)

            for line in rows:
                row = line.strip("\n ").split(",")

                if len(row) == 0:
                    continue

                value_field = row[value_colidx]
                variable_field = row[variable_colidx]
                unit_field = row[unit_colidx]

                # Log unaffected rows (variable field were not fixed/modified) into dest file
                if variable_field not in fixed_variables_or_units:
                    filteredoutputfile.write(line)
                    continue

                # Get min/max value for given variable & unit
                min_value = DataRuleRepository.query_variable_min_value(variable_field, unit_field)
                max_value = DataRuleRepository.query_variable_max_value(variable_field, unit_field)

                # Ignore rows w/out-of-bound values
                if float(value_field) < min_value or float(value_field) > max_value:
                    has_new_issues = True
                    continue

                # Log row w/acceptable value
                filteredoutputfile.write(line)

        return has_new_issues

    @classmethod
    def create(cls, input_entity):
        """Create instance of this class."""
        # Diagnose input data, populate relevant attribs & files.
        # Perform "row checks" on data rows & categorize them as:
        #   1. Rows w/structural issue
        #   2. Rows w/ignored scenario
        #   3. Duplicate rows
        #   4. Accepted rows
        # Ea of this group of rows logged into appropriate dest file
        # For accepted rows, perform "field checks", try to find
        #   1. "Bad" fields
        #   2. "Unknown" fields
        # Log result into appropriate data struct
        # TODO: Reimplement this method with pandas for better performance 
        diagnosis = InputDataDiagnosis()
        diagnosis._initialize_row_destination_files()
        diagnosis._input_entity = input_entity
        
        # Initialize sets to store found labels/fields
        scenario_fields = set()
        region_fields = set()
        variable_fields = set()
        item_fields = set()
        year_fields = set()
        unit_fields = set()
        
        # Initialize variables required to parse input file
        delimiter = input_entity.delimiter
        header_is_included = input_entity.header_is_included
        initial_lines_to_skip = input_entity.initial_lines_to_skip
        scenario_colidx = input_entity.scenario_colnum - 1
        region_colidx = input_entity.region_colnum - 1
        variable_colidx = input_entity.variable_colnum - 1
        item_colidx = input_entity.item_colnum - 1
        unit_colidx = input_entity.unit_colnum - 1
        year_colidx = input_entity.year_colnum - 1
        value_colidx = input_entity.value_colnum - 1
        
        # Update private helper attributes
        diagnosis._update_ncolumns_info(input_entity)

        # Open all row destination files
        # fmt: off
        with \
            open(str(input_entity.file_path), "r") as inputfile, \
            open(str(diagnosis.STRUCTISSUEROWS_DSTPATH), "w+") as structissuefile, \
            open(str(diagnosis.IGNOREDSCENARIOROWS_DSTPATH), "w+") as ignoredscenfile, \
            open(str(diagnosis.DUPLICATESROWS_DSTPATH), "w+") as duplicatesfile, \
            open(str(diagnosis.ACCEPTEDROWS_DSTPATH), "w+") as acceptedfile \
        :
        # fmt: on
            # Get lines & ncolumns info from input file
            lines = inputfile.readlines()

            # Diagnose every line from input file
            for line_index in range(len(lines)):
                line = lines[line_index].strip("\n")
                row = line.split(delimiter)
                rownum = line_index + 1

                # Ignore skipped row
                if rownum <= initial_lines_to_skip:
                    continue

                # Ignore header row
                if (rownum == initial_lines_to_skip + 1) and header_is_included:
                    continue

                # Ignore row that fails a row check
                if diagnosis._diagnose_row(rownum, row, line, structissuefile, ignoredscenfile, duplicatesfile):
                    continue

                # Log accepted row
                diagnosis.nrows_accepted += 1
                acceptedfile.write(line + "\n")

                # Store found labels/fields
                _quotes_and_space = '\'\"` '
                scenario_fields.add(row[scenario_colidx].strip(_quotes_and_space))
                region_fields.add(row[region_colidx].strip(_quotes_and_space))
                variable_fields.add(row[variable_colidx].strip(_quotes_and_space))
                item_fields.add(row[item_colidx].strip(_quotes_and_space))
                unit_fields.add(row[unit_colidx].strip(_quotes_and_space))
                year_fields.add(row[year_colidx].strip(_quotes_and_space))
                
                # Parse value
                diagnosis._diagnose_value_field(row[value_colidx].strip(_quotes_and_space))

        # Diagnose all found fields

        for scenario in scenario_fields:
            diagnosis._diagnose_scenario_field(scenario)

        for region in region_fields:
            diagnosis._diagnose_region_field(region)

        for variable in variable_fields:
            diagnosis._diagnose_variable_field(variable)

        for item in item_fields:
            diagnosis._diagnose_item_field(item)

        for year in year_fields:
            diagnosis._diagnose_year_field(year)

        for unit in unit_fields:
            diagnosis._diagnose_unit_field(unit)

        # Remove duplicates from bad/unknown labels table
        # Note: Didn't simply store labels in a set because label classes aren't safe use w/hashtable-based data struct
        diagnosis.bad_labels = list(set(diagnosis.bad_labels))
        diagnosis.unknown_labels = list(set(diagnosis.unknown_labels))
        return diagnosis

    # Private util methods for row checks

    def _diagnose_row(self, rownum, row, line, structissuefile, ignoredscenfile, duplicatesfile):
        """Check given row for various issues."""
        # If row fails a check, log into appropriate file
        return self._check_row_for_structural_issue(rownum, row, structissuefile) or \
               self._check_row_for_ignored_scenario(rownum, row, ignoredscenfile) or \
               self._check_if_duplicate_row(rownum, line, duplicatesfile)

    def _check_row_for_structural_issue(self, rownum, row, structissuefile):
        """Check if row has structural issue, log into file if so."""
        # Returns result of structural check
        # Structural issue for row:
        #   1. wrong num fields
        #   2. field w/structural issue
        # Structural issue for field:
        #  - varies depending on field type, can be inferred from implementation below
        
        self.nrows_w_struct_issue += 1  # Assume row has structural issue
        
        if len(row) != self._correct_ncolumns:
            self._log_row_w_struct_issue(rownum, row, "Mismatched number of fields", structissuefile)
            return True
        
        if row[self._input_entity.scenario_colnum - 1] == "":
            self._log_row_w_struct_issue(rownum, row, "Empty scenario field", structissuefile)
            return True
        
        if row[self._input_entity.region_colnum - 1] == "":
            self._log_row_w_struct_issue(rownum, row, "Empty region field", structissuefile)
            return True
        
        if row[self._input_entity.variable_colnum - 1] == "":
            self._log_row_w_struct_issue(rownum, row, "Empty variable field", structissuefile)
            return True
        
        if row[self._input_entity.item_colnum - 1] == "":
            self._log_row_w_struct_issue(rownum, row, "Empty item field", structissuefile)
            return True
        
        if row[self._input_entity.unit_colnum - 1] == "":
            self._log_row_w_struct_issue(rownum, row, "Empty unit field", structissuefile)
            return True
        
        year_field = row[self._input_entity.year_colnum - 1]
        
        if year_field == "":
            self._log_row_w_struct_issue(rownum, row, "Empty year field", structissuefile)
            return True
        
        try:
            int(year_field)
        except:
            self._log_row_w_struct_issue(rownum, row, "Non-integer year field", structissuefile)
            return True
        
        if self._check_row_for_value_w_structural_issue(rownum, row, structissuefile):
            return True

        self.nrows_w_struct_issue -= 1  # Subtrac value back if row doesn't have a struct issue
        return False

    def _check_row_for_value_w_structural_issue(self, rownum, row, structissuefile):
        """Check if row has value field w/structural issue, log if so."""
        value_field = row[self._input_entity.value_colnum - 1]
        
        try:
            # Get fixed value
            value_fix = DataRuleRepository.query_fix_from_value_fix_table(value_field)
            value_fix = value_fix if value_fix is not None else value_field

            # Get matching variable
            variable_field = row[self._input_entity.variable_colnum - 1]
            matching_variable = DataRuleRepository.query_matching_variable(variable_field)
            matching_variable = matching_variable if matching_variable is not None else variable_field
            
            # Get matching unit
            unit_field = row[self._input_entity.unit_colnum - 1]
            matching_unit = DataRuleRepository.query_matching_unit(unit_field)
            matching_unit = matching_unit if matching_unit is not None else unit_field
            
            # Get min/max value for the given variable and unit
            min_value = DataRuleRepository.query_variable_min_value(matching_variable, matching_unit)
            max_value = DataRuleRepository.query_variable_max_value(matching_variable, matching_unit)

            if float(value_fix) < min_value:
                issue_text = "Value for variable {} is smaller than {} {}".format(matching_variable, min_value, matching_unit)
                self._log_row_w_struct_issue(rownum, row, issue_text, structissuefile)
                return True

            if float(value_fix) > max_value:
                issue_text = "Value for variable {} is greater than {} {}".format(matching_variable, max_value, matching_unit)
                self._log_row_w_struct_issue(rownum, row, issue_text, structissuefile)
                return True
        except:
            self._log_row_w_struct_issue(rownum, row, "Non-numeric value field", structissuefile)
            return True

        return False

    def _check_row_for_ignored_scenario(self, rownum, row, ignoredscenfile):
        """Check if row contains ignored scenario, log into given file if so."""
        # Returns result of check
        if row[self._input_entity.scenario_colnum - 1] in self._input_entity.scenarios_to_ignore:
            log_row = [str(rownum), *row]
            log_text = ",".join(log_row) + "\n"
            ignoredscenfile.write(log_text)
            self.nrows_w_ignored_scenario += 1
            return True

        return False

    def _check_if_duplicate_row(self, rownum, row, duplicatesfile):
        """Check if row is duplicate, log into duplicates file if so."""
        # Return result of check
        # NOTE: Finding duplicates using in-memory data struct might cause problem if dataset too big
        #       Consider using solutions like SQL
        self._row_occurence_dict.setdefault(row, 0)
        self._row_occurence_dict[row] += 1
        occurence = self._row_occurence_dict[row]

        if occurence > 1:
            log_text = "{},{},{}\n".format(rownum, row, occurence)
            duplicatesfile.write(log_text)
            self.nrows_duplicate += 1
            return True

        return False

    # Private util methods for field/label checks

    def _diagnose_value_field(self, value):
        """Check if value exists in fix table, log if so."""
        fixed_value = DataRuleRepository.query_fix_from_value_fix_table(value)

        if fixed_value is not None:
            float(fixed_value)  # Raise error if non-numeric
            self._log_bad_label(value, self.VALUE_COLNAME, fixed_value)
        else:
            float(value)  # Raise error if non-numeric

    def _diagnose_scenario_field(self, scenario):
        """Check if scenario is bad / unknown, log if so."""
        scenario_w_correct_case = DataRuleRepository.query_matching_scenario(scenario)

        # Correct scenario
        if scenario_w_correct_case == scenario:
            return

        # Unkown scenario
        if scenario_w_correct_case is None:
            closest_scenario = DataRuleRepository.query_partially_matching_scenario(scenario)
            self._log_unknown_label(scenario, self.SCENARIO_COLNAME, closest_scenario)
            return

        # Known scenario but spelled wrong
        if scenario_w_correct_case != scenario:
            self._log_bad_label(scenario, self.SCENARIO_COLNAME, scenario_w_correct_case)

    def _diagnose_region_field(self, region):
        """Check if region is bad or unknown, log if so."""
        region_w_correct_case = DataRuleRepository.query_matching_region(region)

        # Correct region
        if region_w_correct_case == region:
            return

        fixed_region = DataRuleRepository.query_fix_from_region_fix_table(region)

        if (region_w_correct_case is None) and (fixed_region is None):
            # Unknown region
            closest_region = DataRuleRepository.query_partially_matching_region(region)
            self._log_unknown_label(region, self.REGION_COLNAME, closest_region)
        elif (region_w_correct_case != region) and (region_w_correct_case is not None):
            # Known region but spelled wrongly
            self._log_bad_label(region, self.REGION_COLNAME, region_w_correct_case)
        elif fixed_region is not None:
            # Known bad region
            self._log_bad_label(region, self.REGION_COLNAME, fixed_region)

    def _diagnose_variable_field(self, variable):
        """Check if variable is bad or unknown, log if so."""
        variable_w_correct_case = DataRuleRepository.query_matching_variable(variable)
        
        # Correct variable
        if variable_w_correct_case == variable:
            return

        # Unkown variable
        if variable_w_correct_case is None:
            closest_variable = DataRuleRepository.query_partially_matching_variable(variable)
            self._log_unknown_label(variable, self.VARIABLE_COLNAME, closest_variable)
            return

        # Known variable but misspelled
        if variable_w_correct_case != variable:
            self._log_bad_label(variable, self.VARIABLE_COLNAME, variable_w_correct_case)

    def _diagnose_item_field(self, item):
        """Check if item is bad or unknown, log if so."""
        item_w_correct_case = DataRuleRepository.query_matching_item(item)

        # Correct item
        if item_w_correct_case == item:
            return

        # Unkown item
        if item_w_correct_case is None:
            closest_item = DataRuleRepository.query_partially_matching_item(item)
            self._log_unknown_label(item, self.ITEM_COLNAME, closest_item)
            return

        # Known item but misspelled 
        if item_w_correct_case != item:
            self._log_bad_label(item, self.ITEM_COLNAME, item_w_correct_case)

    def _diagnose_year_field(self, year):
        """Check if year is bad or unknown, log if so."""
        # NOTE: Assumes rows w/non-int year value would have failed row structural check,
        #       so following type-cast should never raise an error
        int(year)

        if not DataRuleRepository.query_label_in_years(year):
            self.unknown_years.add(year)  # Unknown years automatically recognized

    def _diagnose_unit_field(self, unit):
        """Check if unit is bad or unknown, log if so."""
        unit_w_correct_case = DataRuleRepository.query_matching_unit(unit)

        # Correct unit
        if unit_w_correct_case == unit:
            return

        # Unkown unit
        if unit_w_correct_case is None:
            closest_unit = DataRuleRepository.query_partially_matching_unit(unit)
            self._log_unknown_label(unit, self.UNIT_COLNAME, closest_unit)
            return

        # Known unit but misspelled
        if unit_w_correct_case != unit:
            self._log_bad_label(unit, self.UNIT_COLNAME, unit_w_correct_case)

    # Private util methods to log found errors/issues

    def _log_row_w_struct_issue(self, rownum, row, issue_description, structissuefile):
        """Return log text for given row w/structural issue."""
        log_ncolumns = self._largest_ncolumns + 2
        log_row = [str(rownum), *row] + ["" for _ in range(log_ncolumns)]
        log_row = log_row[:log_ncolumns]
        log_row[-1] = issue_description
        log_text = ",".join(log_row) + "\n"
        structissuefile.write(log_text)

    def _log_bad_label(self, bad_label, associated_column, fix):
        """Log bad label."""
        self.bad_labels.append(BadLabelInfo(bad_label, associated_column, fix))

    def _log_unknown_label(self, unknown_label, associated_column, closest_label):
        """Log unknown label."""
        # Appending row 1 by 1 to a pandas dataframe is slow, so we store these rows in a list first
        self.unknown_labels.append(UnknownLabelInfo(unknown_label, associated_column, closest_label, fix="", override=False))

    # Other private util methods

    def _initialize_row_destination_files(self):
        """Create/recreate destination files"""

        # Deletes existing files, if any

        if self.STRUCTISSUEROWS_DSTPATH.exists():
            self.STRUCTISSUEROWS_DSTPATH.unlink()

        if self.IGNOREDSCENARIOROWS_DSTPATH.exists():
            self.IGNOREDSCENARIOROWS_DSTPATH.unlink()

        if self.DUPLICATESROWS_DSTPATH.exists():
            self.DUPLICATESROWS_DSTPATH.unlink()

        if self.ACCEPTEDROWS_DSTPATH.exists():
            self.ACCEPTEDROWS_DSTPATH.unlink()

        # Create files
        self.STRUCTISSUEROWS_DSTPATH.touch()
        self.IGNOREDSCENARIOROWS_DSTPATH.touch()
        self.DUPLICATESROWS_DSTPATH.touch()
        self.ACCEPTEDROWS_DSTPATH.touch()

    def _update_ncolumns_info(self, input_entity):
        """Get info about num cols, populate relevant private attribs"""
        self._correct_ncolumns = 0
        largest_ncolumns = 0
        ncolumns_occurence_dict = {}

        with open(str(input_entity.file_path)) as csvfile:
            lines = csvfile.readlines()

            for line in lines:
                ncolumns = len(line.split(input_entity.delimiter))
                ncolumns_occurence_dict.setdefault(ncolumns, 0)
                ncolumns_occurence_dict[ncolumns] += 1
                largest_ncolumns = max(largest_ncolumns, ncolumns)
        most_frequent_ncolumns = max(ncolumns_occurence_dict, key=lambda x: ncolumns_occurence_dict.get(x, -1))

        # Use most frequent ncolumns as proxy for num cols in a clean row
        self._correct_ncolumns = most_frequent_ncolumns
        self._largest_ncolumns = largest_ncolumns


class OutputDataEntity:
    """Domain entity for processed/output data"""

    # Column names of Pandas dataframe that stores processed data
    MODEL_COLNAME = "Model name"
    SCENARIO_COLNAME = "Scenario"
    REGION_COLNAME = "Region"
    VARIABLE_COLNAME = "Variable"
    ITEM_COLNAME = "Item"
    UNIT_COLNAME = "Unit"
    YEAR_COLNAME = "Year"
    VALUE_COLNAME = "Value"

    def __init__(self):
        self.file_path = Path()
        # A Pandas dataframe that store processed data
        # Data frame specification
        #  1. Follows col arrangement dictated by the GlobalEcon team
        #  2. Uses class attribs defined above as col names
        #  3. Stores cols w/numeric type in str, rest as categorical dtype
        self.processed_data: pd.DataFrame = DataFrame()

        # Unique fields in processed dataframe (sorted)
        self.unique_scenarios = []
        self.unique_regions = []
        self.unique_variables = []
        self.unique_items = []
        self.unique_years = []

    def get_value_trends_table(self, scenario, region, variable):
        """Return table for value trends visualization or None."""
        # Table built from processed data, args provided specify how processed data sliced
        processed_data = self.processed_data

        # Slice & copy data frame based on arguments
        sliced_data = processed_data.loc[
            (processed_data[self.SCENARIO_COLNAME] == scenario)
            & (processed_data[self.REGION_COLNAME] == region)
            & (processed_data[self.VARIABLE_COLNAME] == variable)
        ].copy()

        # Return if sliced data is empty
        if sliced_data.shape[0] == 0:
            self.valuetrends_viz_table = None
            return

        # Convert year & value col to numeric
        sliced_data[self.YEAR_COLNAME] = pd.to_numeric(sliced_data[self.YEAR_COLNAME])
        sliced_data[self.VALUE_COLNAME] = pd.to_numeric(sliced_data[self.VALUE_COLNAME])
        return sliced_data.groupby(self.ITEM_COLNAME)

    def get_growth_trends_table(self, scenario, region, variable):
        """Return table for growth trends visualization or None."""
        # Table built from processed data, args provided specify how processed data should be sliced
        processed_data = self.processed_data

        # Slice & copy data frame based on arguments
        sliced_data = processed_data.loc[
            (processed_data[self.SCENARIO_COLNAME] == scenario)
            & (processed_data[self.REGION_COLNAME] == region)
            & (processed_data[self.VARIABLE_COLNAME] == variable)
        ].copy()

        # Return if sliced data is empty
        if sliced_data.shape[0] == 0:
            self.valuetrends_viz_table = None
            return

        # Convert year & value col to numeric
        sliced_data[self.YEAR_COLNAME] = pd.to_numeric(sliced_data[self.YEAR_COLNAME])
        sliced_data[self.VALUE_COLNAME] = pd.to_numeric(sliced_data[self.VALUE_COLNAME])
        
        return sliced_data.groupby(self.ITEM_COLNAME)

    @classmethod
    def create(cls, input_entity, input_diagnosis):
        """Create instance of this class."""
        # TODO: Consider abstracting some logic into Factory class & Service class
        # Read from accepted rows destination file
        # File: 
        #  - Should have no header row or lines to skip
        #  - Shouldn't have recs w/any row issues 
        #  - But may still contain recs w/fixable field issues. 
        #  - Recs in this file shouldn't have additional or removed cols
        processed_data = pd.read_csv(input_diagnosis.ACCEPTEDROWS_DSTPATH, delimiter=input_entity.delimiter, header=None, dtype=object) # type: ignore
        
        # Make sure the data frame has all the required 8 columns (not more) in the correct arrangement
        colnames = processed_data.columns
        colnames = [
            colnames[input_entity.scenario_colnum - 1],
            colnames[input_entity.region_colnum - 1],
            colnames[input_entity.variable_colnum - 1],
            colnames[input_entity.item_colnum - 1],
            colnames[input_entity.unit_colnum - 1],
            colnames[input_entity.year_colnum - 1],
            colnames[input_entity.value_colnum - 1],
        ]
        processed_data = processed_data[colnames]
        processed_data.insert(0, cls.MODEL_COLNAME, input_entity.model_name)
        
        # Rename data frame columns
        processed_data.columns = [
            cls.MODEL_COLNAME,
            cls.SCENARIO_COLNAME,
            cls.REGION_COLNAME,
            cls.VARIABLE_COLNAME,
            cls.ITEM_COLNAME,
            cls.UNIT_COLNAME,
            cls.YEAR_COLNAME,
            cls.VALUE_COLNAME
        ]
        
        # Reassign col dtypes
        # Note: numeric cols stored as str b/c might have values like NA, N/A, #DIV/0! etc
        processed_data[cls.SCENARIO_COLNAME] = processed_data[cls.SCENARIO_COLNAME].astype("category")
        processed_data[cls.REGION_COLNAME] = processed_data[cls.REGION_COLNAME].astype("category")
        processed_data[cls.VARIABLE_COLNAME] = processed_data[cls.VARIABLE_COLNAME].astype("category")
        processed_data[cls.ITEM_COLNAME] = processed_data[cls.ITEM_COLNAME].astype("category")
        processed_data[cls.YEAR_COLNAME] = processed_data[cls.YEAR_COLNAME].apply(str)  # TODO: Will this affect performance?
        processed_data[cls.VALUE_COLNAME] = processed_data[cls.VALUE_COLNAME].apply(str)
        processed_data[cls.UNIT_COLNAME] = processed_data[cls.UNIT_COLNAME].astype("category")
        
        # Bad / unknown label mapping dictionaries
        scenariomapping = {}
        regionmapping = {}
        variablemapping = {}
        itemmapping = {}
        unitmapping = {}
        yearmapping = {}
        valuemapping = {}
        
        # Dropped labels set
        droppedscenarios = set()
        droppedregions = set()
        droppedvariables = set()
        droppeditems = set()
        droppedyears = set()
        droppedunits = set()
        droppedvalues = set()

        # Populate label mapping dicts based on info about bad labels
        for bad_label_info in input_diagnosis.bad_labels:
            label = bad_label_info.label
            associatedcol = bad_label_info.associated_column
            fix = bad_label_info.fix

            if associatedcol == input_diagnosis.SCENARIO_COLNAME:
                scenariomapping[label] = fix
            elif associatedcol == input_diagnosis.REGION_COLNAME:
                regionmapping[label] = fix
            elif associatedcol == input_diagnosis.VARIABLE_COLNAME:
                variablemapping[label] = fix
            elif associatedcol == input_diagnosis.ITEM_COLNAME:
                itemmapping[label] = fix
            elif associatedcol == input_diagnosis.UNIT_COLNAME:
                unitmapping[label] = fix
            elif associatedcol == input_diagnosis.YEAR_COLNAME:
                yearmapping[label] = fix
            elif associatedcol == input_diagnosis.VALUE_COLNAME:
                valuemapping[label] = fix

        # Populate label mapping dicts & dropped labels set based on info about unknown labels
        for unknown_label_info in input_diagnosis.unknown_labels:
            label = unknown_label_info.label
            associatedcol = unknown_label_info.associated_column
            fix = unknown_label_info.fix
            override = unknown_label_info.override

            if fix != "":
                # Remember fixes
                if associatedcol == input_diagnosis.SCENARIO_COLNAME:
                    scenariomapping[label] = fix
                elif associatedcol == input_diagnosis.REGION_COLNAME:
                    regionmapping[label] = fix
                elif associatedcol == input_diagnosis.VARIABLE_COLNAME:
                    variablemapping[label] = fix
                elif associatedcol == input_diagnosis.ITEM_COLNAME:
                    itemmapping[label] = fix
                elif associatedcol == input_diagnosis.UNIT_COLNAME:
                    unitmapping[label] = fix
                elif associatedcol == input_diagnosis.YEAR_COLNAME:
                    yearmapping[label] = fix
                elif associatedcol == input_diagnosis.VALUE_COLNAME:
                    valuemapping[label] = fix
            elif not override:
                # Remember labels to be dropped
                if associatedcol == input_diagnosis.SCENARIO_COLNAME:
                    droppedscenarios.add(label)
                elif associatedcol == input_diagnosis.REGION_COLNAME:
                    droppedregions.add(label)
                elif associatedcol == input_diagnosis.VARIABLE_COLNAME:
                    droppedvariables.add(label)
                elif associatedcol == input_diagnosis.ITEM_COLNAME:
                    droppeditems.add(label)
                elif associatedcol == input_diagnosis.YEAR_COLNAME:
                    droppedyears.add(label)
                elif associatedcol == input_diagnosis.UNIT_COLNAME:
                    droppedunits.add(label)
                elif associatedcol == input_diagnosis.VALUE_COLNAME:
                    droppedvalues.add(label)
        try:
            # Apply label fixes
            processed_data[cls.SCENARIO_COLNAME] = processed_data[cls.SCENARIO_COLNAME].apply(lambda x: scenariomapping[x] if x in scenariomapping.keys() else x)
            processed_data[cls.REGION_COLNAME] = processed_data[cls.REGION_COLNAME].apply(lambda x: regionmapping[x] if x in regionmapping.keys() else x)
            processed_data[cls.VARIABLE_COLNAME] = processed_data[cls.VARIABLE_COLNAME].apply(lambda x: variablemapping[x] if x in variablemapping.keys() else x)
            processed_data[cls.ITEM_COLNAME] = processed_data[cls.ITEM_COLNAME].apply(lambda x: itemmapping[x] if x in itemmapping.keys() else x)
            processed_data[cls.UNIT_COLNAME] = processed_data[cls.UNIT_COLNAME].apply(lambda x: unitmapping[x] if x in unitmapping.keys() else x)
            processed_data[cls.YEAR_COLNAME] = processed_data[cls.YEAR_COLNAME].apply(lambda x: yearmapping[x] if x in yearmapping.keys() else x)
            processed_data[cls.VALUE_COLNAME] = processed_data[cls.VALUE_COLNAME].apply(lambda x: valuemapping[x] if x in valuemapping.keys() else x)
            
            # Drop records containing dropped labels
            processed_data = processed_data[processed_data[cls.SCENARIO_COLNAME].apply(lambda x: x not in droppedscenarios)]
            processed_data = processed_data[processed_data[cls.REGION_COLNAME].apply(lambda x: x not in droppedregions)]
            processed_data = processed_data[processed_data[cls.VARIABLE_COLNAME].apply(lambda x: x not in droppedvariables)]
            processed_data = processed_data[processed_data[cls.ITEM_COLNAME].apply(lambda x: x not in droppeditems)]
            processed_data = processed_data[processed_data[cls.YEAR_COLNAME].apply(lambda x: x not in droppedyears)]
            processed_data = processed_data[processed_data[cls.UNIT_COLNAME].apply(lambda x: x not in droppedunits)]
            processed_data = processed_data[processed_data[cls.VALUE_COLNAME].apply(lambda x: x not in droppedvalues)]
        except Exception:
            return None

        # Create entity
        output_entity = OutputDataEntity()
        output_entity.processed_data = processed_data
        output_entity.file_path = DOWNLOADDIR_PATH / (
            Path(input_entity.file_path).stem + datetime.now().strftime("_%m%d%Y_%H%M%S").upper() + ".csv"
        )
        
        # Store processed data in downloadable file
        processed_data.to_csv(output_entity.file_path, header=False, index=False)
        
        # Populate list of unique fields
        cls._populate_unique_fields(output_entity)

        return output_entity

    @classmethod
    def create_from_rediagnosed_n_filtered_output_data(cls, input_entity, input_data_diagnosis):
        """Return output data entity using existing output dataset thats been rediagnosed & filtered."""

        # Read from accepted rows dest file
        processed_data = pd.read_csv(input_data_diagnosis.FILTERED_OUTPUT_DSTPATH, delimiter=input_entity.delimiter, header=None, dtype=object) # type: ignore

        # Rename data frame cols
        processed_data.columns = [
            cls.MODEL_COLNAME,
            cls.SCENARIO_COLNAME,
            cls.REGION_COLNAME,
            cls.VARIABLE_COLNAME,
            cls.ITEM_COLNAME,
            cls.UNIT_COLNAME,
            cls.YEAR_COLNAME,
            cls.VALUE_COLNAME
        ]

        # Reassign column dtypes
        # Note: numeric cols stored as str b/c might have values like NA, N/A, #DIV/0! etc
        processed_data[cls.SCENARIO_COLNAME] = processed_data[cls.SCENARIO_COLNAME].astype("category")
        processed_data[cls.REGION_COLNAME] = processed_data[cls.REGION_COLNAME].astype("category")
        processed_data[cls.VARIABLE_COLNAME] = processed_data[cls.VARIABLE_COLNAME].astype("category")
        processed_data[cls.ITEM_COLNAME] = processed_data[cls.ITEM_COLNAME].astype("category")
        processed_data[cls.YEAR_COLNAME] = processed_data[cls.YEAR_COLNAME].apply(str)  # TODO: Affect performance?
        processed_data[cls.VALUE_COLNAME] = processed_data[cls.VALUE_COLNAME].apply(str)
        processed_data[cls.UNIT_COLNAME] = processed_data[cls.UNIT_COLNAME].astype("category")
        
        # Create entity
        output_entity = OutputDataEntity()
        output_entity.processed_data = processed_data
        output_entity.file_path = DOWNLOADDIR_PATH / (
            Path(input_entity.file_path).stem + datetime.now().strftime("_%m%d%Y_%H%M%S").upper() + ".csv"
        )

        # Store processed data in downloadable file
        processed_data.to_csv(output_entity.file_path, header=False, index=False)
        
        # Populate list of unique fields
        cls._populate_unique_fields(output_entity)
        
        return output_entity

    @classmethod
    def _populate_unique_fields(cls, output_entity):
        """"Populate lists of unique fields retrieved from processed data frame."""
        # Each list must be sorted.
        # NOTE: Some data frame columns have categorical data type, cannot be sorted immed., 
        #       convert those cols into ndarrays first.
        output_entity.unique_scenarios = np.asarray(output_entity.processed_data[output_entity.SCENARIO_COLNAME].unique()).tolist()
        output_entity.unique_scenarios.sort()
        output_entity.unique_regions = np.asarray(output_entity.processed_data[output_entity.REGION_COLNAME].unique()).tolist()
        output_entity.unique_regions.sort()
        output_entity.unique_variables = np.asarray(output_entity.processed_data[output_entity.VARIABLE_COLNAME].unique()).tolist()
        output_entity.unique_variables.sort()
        output_entity.unique_items = np.asarray(output_entity.processed_data[output_entity.ITEM_COLNAME].unique()).tolist()
        output_entity.unique_items.sort()
        output_entity.unique_years = np.asarray(output_entity.processed_data[output_entity.YEAR_COLNAME].unique()).tolist()
        output_entity.unique_years.sort()
        output_entity.unique_years = np.asarray(output_entity.processed_data[output_entity.UNIT_COLNAME].unique()).tolist()
        output_entity.unique_years.sort()


class DataRuleRepository:
    """Provide access to data rules from spreadsheet."""

    __spreadsheet = None  # Spreadsheet containing labels info

    # Valid labels table
    _model_table = None
    _scenario_table = None
    _region_table = None
    _variable_table = None
    _item_table = None
    _unit_table = None
    _year_table = None

    # Fix tables
    _regionfix_table = None
    _valuefix_table = None

    # Constraint tables
    __variableunitvalue_table = None

    # Valid columns
    _model_names = None
    _scenarios = None
    _regions = None
    _variables = None
    _items = None
    _units = None
    _years = None

    # Data structure for critical queries
    _matchingunit_memo = {}
    _matchingvariable_memo = {}
    _valuefix_memo = {}
    _variable_minvalue_memo = {}
    _variable_maxvalue_memo = {}

    @classmethod
    def load(cls, shared_path, proj_path, subdir_path):
        """Read rule xls, load class vars."""

        # Spreadsheet containing labels information
        cls.__spreadsheet = pd.read_excel(
            os.path.join(shared_path, proj_path, subdir_path, ".rules", "RuleTables.xlsx"),
            engine="openpyxl",
            sheet_name=None,
            keep_default_na=False,
        )

        # Valid labels table
        cls._model_table = cls.__spreadsheet["ModelTable"]
        cls._scenario_table = cls.__spreadsheet["ScenarioTable"]
        cls._region_table = cls.__spreadsheet["RegionTable"]
        cls._variable_table = cls.__spreadsheet["VariableTable"]
        cls._item_table = cls.__spreadsheet["ItemTable"]
        cls._unit_table = cls.__spreadsheet["UnitTable"]
        cls._year_table = cls.__spreadsheet["YearTable"]

        # Fix tables
        cls._regionfix_table = cls.__spreadsheet["RegionFixTable"]
        cls._valuefix_table = cls.__spreadsheet["ValueFixTable"]

        # Constraint tables
        cls.__variableunitvalue_table = cls.__spreadsheet["VariableUnitValueTable"]

        # Valid cols
        cls._model_names = set(cls._model_table["Model"].astype("str"))
        cls._scenarios = set(cls._scenario_table["Scenario"].astype("str"))
        cls._regions = set(cls._region_table["Region"].astype("str"))
        cls._variables = set(cls._variable_table["Variable"].astype("str"))
        cls._items = set(cls._item_table["Item"].astype("str"))
        cls._units = set(cls._unit_table["Unit"].astype("str"))
        cls._years = set(cls._year_table["Year"].astype("str"))

        # Data struct for critical queries
        cls._valuefix_memo = dict(cls._valuefix_table.iloc[:, 1:].values)  # Load dataframe as dict

        # Populate data structs for critical queries

        # - Populate matching unit memo
        for unit in cls._units:
            cls._matchingunit_memo[unit.lower()] = unit

        # - Populate matching variable memo
        for variable in cls._variables:
            cls._matchingvariable_memo[variable.lower()] = variable

        # - Populate value-fix memo
        for key in cls._valuefix_memo.keys():
            cls._valuefix_memo[key] = str(cls._valuefix_memo[key])  # store numbers as strings

        # - Populate variable's min/max value memo
        for namedtuple in cls.__variableunitvalue_table.itertuples(index=False):
            
            # Get required variables
            variable = namedtuple.Variable
            unit = namedtuple.Unit
            minvalue = namedtuple[cls.__variableunitvalue_table.columns.get_loc("Minimum Value")]
            maxvalue = namedtuple[cls.__variableunitvalue_table.columns.get_loc("Maximum Value")]
            
            # Update memo
            cls._variable_minvalue_memo[(variable, unit)] = minvalue
            cls._variable_maxvalue_memo[(variable, unit)] = maxvalue

    @classmethod
    def query_model_names(cls):
        """Get all valid model names."""
        result = list(cls._model_names)
        result.sort()
        return result

    @classmethod
    def query_scenarios(cls):
        """Get all valid scenarios."""
        result = list(cls._scenarios)
        result.sort()
        return result

    @classmethod
    def query_regions(cls):
        """Get all valid regions."""
        result = list(cls._regions)
        result.sort()
        return result

    @classmethod
    def query_variables(cls):
        """Get all valid variables"""
        result = list(cls._variables)
        result.sort()
        return result

    @classmethod
    def query_items(cls):
        """Get all valid items """
        result = list(cls._items)
        result.sort()
        return result

    @classmethod
    def query_units(cls):
        """Get all valid units """
        result = list(cls._units)
        result.sort()
        return result

    @classmethod
    def query_label_in_model_names(cls, label):
        """Check if arg exists in model name table."""
        return label in cls._model_names

    @classmethod
    def query_label_in_scenarios(cls, label):
        """Check if arg exists in scenario table."""
        return label in cls._scenarios

    @classmethod
    def query_label_in_regions(cls, label):
        """Check if the argument exists in the region table"""
        return label in cls._regions

    @classmethod
    def query_label_in_variables(cls, label):
        """Check if the argument exists in the variable table"""
        return label in cls._variables

    @classmethod
    def query_label_in_items(cls, label):
        """Check if the argument exists in the item table"""
        return label in cls._items

    @classmethod
    def query_label_in_units(cls, label):
        """Check if the argument exists in the unit table"""
        return label in cls._units

    @classmethod
    def query_label_in_years(cls, label):
        """Check if the argument exists in the years table"""
        return label in cls._years

    @classmethod
    def query_matching_scenario(cls, scenario):
        """Return scenario w/exact case-insensitive spelling as argument, or None."""
        scenario = scenario.lower()
        table = cls._scenario_table
        table = table[table["Scenario"].str.lower() == scenario]

        if table.shape[0] != 0:
            return str(table.iloc[0]["Scenario"])  # type: ignore

        return None

    @classmethod
    def query_partially_matching_scenario(cls, scenario):
        """Return  scenario w/closest spelling to arg."""
        matches = difflib.get_close_matches(scenario, cls._scenarios, n=1, cutoff=0)
        return matches[0]

    @classmethod
    def query_matching_region(cls, region):
        """Return region w/exact case-insensitive spelling as argument, or None."""
        region = region.lower()
        table = cls._region_table
        table = table[table["Region"].str.lower() == region]

        if table.shape[0] != 0:
            return str(table.iloc[0]["Region"])  # type: ignore
        
        return None

    @classmethod
    def query_partially_matching_region(cls, region):
        """Return region w/closest spelling to arg."""
        matches = difflib.get_close_matches(region, cls._regions, n=1, cutoff=0)
        return matches[0]

    @classmethod
    def query_matching_variable(cls, variable):
        """Returns variable w/exact case-insensitive spelling as arg, or None."""
        variable = variable.lower()
        try:
            return cls._matchingvariable_memo[variable]
        except:
            return None

    @classmethod
    def query_partially_matching_variable(cls, variable):
        """Return variable w/closest spelling to arg."""
        matches = difflib.get_close_matches(variable, cls._variables, n=1, cutoff=0)
        return matches[0]

    @classmethod
    def query_matching_item(cls, item):
        """Return item w/exact case-insensitive spelling as arg, or None."""
        item = item.lower()
        table = cls._item_table
        table = table[table["Item"].str.lower() == item]

        if table.shape[0] != 0:
            return str(table.iloc[0]["Item"])  # type: ignore
        
        return None

    @classmethod
    def query_partially_matching_item(cls, item):
        """Return item w/closest spelling to arg."""
        matches = difflib.get_close_matches(item, cls._items, n=1, cutoff=0)
        return matches[0]

    @classmethod
    def query_matching_unit(cls, unit):
        """Return unit w/exact case-insensitive spelling as arg, or None."""
        unit = unit.lower()

        try:
            return cls._matchingunit_memo[unit]
        except:
            return None

    @classmethod
    def query_partially_matching_unit(cls, unit):
        """Return unit w/closest spelling to arg."""
        matches = difflib.get_close_matches(unit, cls._units, n=1, cutoff=0)
        return matches[0]

    @classmethod
    def query_fix_from_value_fix_table(cls, value):
        """Check if fix in fix table, return it or None."""
        fix_table = cls._valuefix_memo

        try:
            return fix_table[value.lower()]
        except:
            return None

    @classmethod
    def query_fix_from_region_fix_table(cls, region):
        """Check if fix in fix table, return it or None."""
        fix_table = cls._regionfix_table

        # Get all rows containing fix
        fix_table = fix_table[fix_table["Region"] == region.lower()]

        # Fix was found
        if fix_table.shape[0] != 0:
            return str(fix_table.iloc[0]["Fix"])

        return None

    @classmethod
    def query_variable_min_value(cls, variable, unit) -> float:
        """Return min value for variable."""
        if (variable, unit) in cls._variable_minvalue_memo.keys():
            return float(cls._variable_minvalue_memo[(variable, unit)])
        else:
            return -math.inf

    @classmethod
    def query_variable_max_value(cls, variable, unit) -> float:
        """Return max value for variable."""
        if (variable, unit) in cls._variable_maxvalue_memo.keys():
            return float(cls._variable_maxvalue_memo[(variable, unit)])
        else:
            return +math.inf
