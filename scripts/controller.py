from .utils import *


class Controller:

    def intro(self, model, view):  # type: ignore # noqa
        """Introduce MVC modules to each other"""
        self.model = model
        self.view = view

    def start(self):
        """Load data, build UI"""
        self.view.display()

        # NOTE To debug ncomment testing code below, avoids problems during jupyter lab debuging
        # self.model.uploadedfile_name = 'globiom.csv'  # <-- Replace w/test input file name
        # self.view.show_notification(Notification.SUCCESS, Notification.FILE_UPLOAD_SUCCESS)
        # self.view.update_file_upload_page()
        # self._reset_later_pages()

    def _reset_later_pages(self):
        """Set the current page as the last/furthest active page"""        
        if not self.model.furthest_active_user_page == self.model.current_user_page:
            self.model.furthest_active_user_page = self.model.current_user_page
            self.view.update_base_app()

    # Base app callbacks

    def onclick_user_mode_btn(self, _):
        """User mode button was clicked"""
        
        if self.model.is_user_an_admin:
            self.model.application_mode = ApplicationMode.ADMIN
            self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
            self.view.update_base_app()
            self.view.modify_cursor_style(None)
        else:
            self.view.show_notification(Notification.ERROR, "Your account does not have admin privileges")

    def onclick_admin_mode_btn(self, _):
        """Admin mode button was clicked"""
        self.model.application_mode = ApplicationMode.USER
        self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
        self.view.update_base_app()
        self.view.modify_cursor_style(None)

    # File upload page callbacks

    def onchange_ua_file_label(self, change):
        """Value of the hidden file label in upload area (ua) changed"""
        file_name: str = change["new"]
        
        if len(file_name) == 0:  # Change triggered by View's internal operation, not by user aciton
            return
        
        self.model.uploadedfile_name = file_name
        
        if file_name.endswith(".csv"):
            self.view.show_notification(Notification.SUCCESS, Notification.FILE_UPLOAD_SUCCESS)
            self.view.update_file_upload_page()
        else:
            self.view.show_notification(Notification.ERROR, Notification.INVALID_FILE_FORMAT)
            self.model.remove_uploaded_file()
            self.model.uploadedfile_name = ""
        
        self._reset_later_pages()

    def onclick_remove_file(self, _):
        """'x' button in file upload snackbar was clicked"""
        self.model.remove_uploaded_file()
        self.model.uploadedfile_name = ""
        self.view.update_file_upload_page()
        self._reset_later_pages()

    def onchange_associated_projects(self, change):
        """Selections for associated projects changed"""
        self.model.associated_project_dirnames = list(change["new"])
        self._reset_later_pages()
        self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
        self.model.load_rules(self.model.associated_project_dirnames[0])
        self.view.modify_cursor_style(None)

    def onclick_next_from_upage_1(self, _):
        """'Next' button on the file upload page was clicked"""
        
        if len(self.model.uploadedfile_name) == 0:
            self.view.show_notification(Notification.INFO, Notification.PLEASE_UPLOAD)
            return
        
        if len(self.model.associated_project_dirnames) == 0:
            self.view.show_notification(Notification.INFO, "Please select the associated projects")
            return
        
        self.model.current_user_page = UserPage.DATA_SPECIFICATION
        
        if self.model.furthest_active_user_page == UserPage.FILE_UPLOAD:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
            self.model.furthest_active_user_page = UserPage.DATA_SPECIFICATION
            error_message = self.model.init_data_specification_page_states(self.model.uploadedfile_name)
            self.view.update_data_specification_page()

            if error_message is not None:
                self.view.show_notification(Notification.ERROR, error_message)
            else:
                self.view.show_notification(Notification.INFO, Notification.FIELDS_WERE_PREPOPULATED)
        
        self.view.update_base_app()
        self.view.modify_cursor_style(None)

    # Data specification page callbacks

    def onchange_model_name_dropdown(self, change):
        """Selection in 'model name' dropdown changed"""
        # The event is triggered programmatically by page update, and not by dropdown selection

        if not change["new"] == self.model.model_name:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.model_name = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_header_is_included_checkbox(self, change):
        """The state of 'header is included' checkbox changed"""
        # Event triggered programmatically by page update, not by checkbox selection
        if not change["new"] == self.model.header_is_included:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.header_is_included = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_lines_to_skip_text(self, change):
        """Content of 'lines to skip' text changed"""
        new_value = change["new"]
        self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)

        try:
            new_value = int(new_value)
            # Event triggered programmatically by page update, not by user action

            if new_value == self.model.lines_to_skip:
                return

            if new_value < 0:
                self.view.show_notification(Notification.WARNING, "Number of lines cannot be negative")
                new_value = 0

            self.model.lines_to_skip = new_value
        except:
            self.view.show_notification(Notification.WARNING, "Invalid number of lines")
            self.model.lines_to_skip = 0

        self.view.update_data_specification_page()
        self.view.modify_cursor_style(None)
        self._reset_later_pages()

    def onchange_delimiter_dropdown(self, change):
        """Selection in 'delimiter' dropdown changed."""
        # Event triggered programmatically by page update, not by dropdown selection
        if not Delimiter.get_model(change["new"]) == self.model.delimiter:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.delimiter = Delimiter.get_model(change["new"])
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_scenarios_to_ignore_text(self, change):
        """Content of 'scenarios to ignore' text changed"""
        # Event triggered programmatically by page update, not by user actions
        if not change["new"] == self.model.scenarios_to_ignore_str:
            self.model.scenarios_to_ignore_str = change["new"]
            self.view.update_data_specification_page()
            self._reset_later_pages()

    def onchange_scenario_column_dropdown(self, change):
        """Selection in 'scenario column' dropdown was changed"""
        # Event triggered programmatically by page update, instead of user action
        if not change["new"] == self.model.assigned_scenario_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_scenario_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_region_column_dropdown(self, change):
        """The selection in 'region column' dropdown changed"""
        # Event triggered programmatically by page update, instead of user action
        if not change["new"] == self.model.assigned_region_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_region_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_variable_column_dropdown(self, change):
        """The selection in 'variable column' dropdown changed"""
        # Event triggered programmatically by page update, instead user action
        if not change["new"] == self.model.assigned_variable_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_variable_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_item_column_dropdown(self, change):
        """Selection in 'item column' dropdown changed"""
        if not change["new"] == self.model.assigned_item_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_item_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_unit_column_dropdown(self, change):
        """The selection in 'unit column' dropdown changed"""
        if not change["new"] == self.model.assigned_unit_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_unit_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_year_column_dropdown(self, change):
        """The selection in 'year column' dropdown changed"""
        if not change["new"] == self.model.assigned_year_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_year_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onchange_value_column_dropdown(self, change):
        """The selection in 'value column' dropdown changed"""
        if not change["new"] == self.model.assigned_value_column:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.assigned_value_column = change["new"]
            self.view.update_data_specification_page()
            self.view.modify_cursor_style(None)
            self._reset_later_pages()

    def onclick_previous_from_upage_2(self, _):
        """'Previous' button on the data specification page was clicked"""
        self.model.current_user_page = UserPage.FILE_UPLOAD
        self.view.update_base_app()

    def onclick_next_from_upage_2(self, _):
        """'Next' button on the data specification page was clicked"""
        warning_message = self.model.validate_data_specification_input()

        if warning_message is not None:
            self.view.show_notification(Notification.WARNING, warning_message)
            return

        self.model.current_user_page = UserPage.INTEGRITY_CHECKING

        if self.model.furthest_active_user_page == UserPage.DATA_SPECIFICATION:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
            self.model.furthest_active_user_page = UserPage.INTEGRITY_CHECKING
            self.model.init_integrity_checking_page_states()
            self.view.update_integrity_checking_page()

        self.view.update_base_app()
        self.view.modify_cursor_style(None)

    # Integrity checking page callbacks

    def onchange_fix_dropdown(self, change, row_index):
        """Selection for one of the fix dropdowns in unknown labels table changed"""
        DROPDOWN_INDEX = 3

        if not change["new"] == self.model.unknown_labels_overview_tbl[row_index][DROPDOWN_INDEX]:
            self.model.unknown_labels_overview_tbl[row_index][DROPDOWN_INDEX] = change["new"]
            self._reset_later_pages()

    def onchange_override_checkbox(self, change, row_index):
        """The selection for one of the override checkbox in unknown labels table was changed"""
        CHECKBOX_INDEX = 4

        if not change["new"] == self.model.unknown_labels_overview_tbl[row_index][CHECKBOX_INDEX]:
            self.model.unknown_labels_overview_tbl[row_index][CHECKBOX_INDEX] = change["new"]
            self._reset_later_pages()

    def onclick_previous_from_upage_3(self, _):
        """'Previous' button on the data specification page was clicked"""
        self.model.current_user_page = UserPage.DATA_SPECIFICATION
        self.view.update_base_app()

    def onclick_next_from_upage_3(self, _):
        """'Next' button on the data specification page was clicked"""
        warning_message = self.model.validate_unknown_labels_table(self.model.unknown_labels_overview_tbl)

        if warning_message is not None:
            self.view.show_notification(Notification.WARNING, warning_message)
            return

        self.model.current_user_page = UserPage.PLAUSIBILITY_CHECKING

        if self.model.furthest_active_user_page == UserPage.INTEGRITY_CHECKING:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
            self.model.furthest_active_user_page = UserPage.PLAUSIBILITY_CHECKING
            popup_message = self.model.init_plausibility_checking_page_states(self.model.unknown_labels_overview_tbl)

            if popup_message:
                self.view.show_modal_dialog("Re-Diagnose Result", popup_message)

                if popup_message[:5] == 'ERROR':
                    self.model.current_user_page = UserPage.INTEGRITY_CHECKING

            if self.model.current_user_page == UserPage.PLAUSIBILITY_CHECKING:
                self.view.update_plausibility_checking_page()
                self.view.update_value_trends_chart()
                self.view.update_growth_trends_chart()

        self.view.update_base_app()
        self.view.modify_cursor_style(None)

    # Plausibility checking page callbacks

    def onclick_value_trends_tab(self, _):
        """Value trends tab clicked"""
        self.model.active_visualization_tab = VisualizationTab.VALUE_TRENDS
        self.view.update_plausibility_checking_page()

    def onchange_valuetrends_scenario(self, change):
        """Scenario selection for value trends visualization changed"""
        self.model.valuetrends_scenario = change["new"]

    def onchange_valuetrends_region(self, change):
        """Region selection for value trends visualization changed"""
        self.model.valuetrends_region = change["new"]

    def onchange_valuetrends_variable(self, change):
        """Variable selection for value trends visualization changed"""
        self.model.valuetrends_variable = change["new"]

    def onclick_visualize_value_trends(self, _):
        """Visualize button was clicked"""
        if self.model.valuetrends_scenario == "":
            self.view.show_notification(Notification.WARNING, "Scenario cannot be empty")
        elif self.model.valuetrends_region == "":
            self.view.show_notification(Notification.WARNING, "Region cannot be empty")
        elif self.model.valuetrends_variable == "":
            self.view.show_notification(Notification.WARNING, "Variable cannot be empty")
        else:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.update_valuetrends_visualization_states()
            self.view.update_value_trends_chart()
            self.view.modify_cursor_style(None)
            self.view.show_notification(Notification.SUCCESS, "Visualized value trends")

    def onclick_growth_trends_tab(self, widget):
        """Growth trends tab was clicked"""
        self.model.active_visualization_tab = VisualizationTab.GROWTH_TRENDS
        self.view.update_plausibility_checking_page()

    def onchange_growthtrends_scenario(self, change):
        """Scenario selection for growth trends visualization was changed"""
        self.model.growthtrends_scenario = change["new"]

    def onchange_growthtrends_region(self, change):
        """Region selection for growth trends visualization was changed"""
        self.model.growthtrends_region = change["new"]

    def onchange_growthtrends_variable(self, change):
        """Variable selection for growth trends visualization was changed"""
        self.model.growthtrends_variable = change["new"]

    def onclick_visualize_growth_trends(self, _):
        """Visualize button was clicked"""

        if self.model.growthtrends_scenario == "":
            self.view.show_notification(Notification.WARNING, "Scenario cannot be empty")
        elif self.model.growthtrends_region == "":
            self.view.show_notification(Notification.WARNING, "Region cannot be empty")
        elif self.model.growthtrends_variable == "":
            self.view.show_notification(Notification.WARNING, "Variable cannot be empty")
        else:
            self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
            self.model.update_growthtrends_visualization_states()
            self.view.update_growth_trends_chart()
            self.view.modify_cursor_style(None)
            self.view.show_notification(Notification.SUCCESS, "Visualized growth trends")

    def onclick_restart_submission(self, _):
        """The restart submission icon button was clicked"""
        self.model.current_user_page = UserPage.FILE_UPLOAD
        self.model.furthest_active_user_page = UserPage.FILE_UPLOAD
        self.view.modify_cursor_style(CSS.CURSOR_MOD__WAIT)
        self.view.update_base_app()
        self.view.modify_cursor_style(None)

    def onclick_previous_from_upage_4(self, _):
        """The 'previous' button in the last page was clicked"""
        self.model.current_user_page = UserPage.INTEGRITY_CHECKING
        self.view.update_base_app()

    def onclick_submit(self, _):
        """The 'submit' button in the last page was clicked"""
        self.view.modify_cursor_style(CSS.CURSOR_MOD__PROGRESS)
        self.model.submit_processed_file()
        self.view.modify_cursor_style(None)
        self.view.show_notification(Notification.SUCCESS, "Your file has been successfully submitted")

        if self.model.overridden_labels > 0:
            self.view.show_modal_dialog(
                "Pending Submission Approval",
                "Your file has been submitted, but it is being placed under review because it contains overridden "
                "labels. Reach out to Dominique (vandermd@purdue.edu) about this submission so he can begin the "
                "review process.",
            )
