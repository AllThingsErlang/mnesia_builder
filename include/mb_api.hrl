
-ifndef(DB_API_HRL).
-define(DB_API_HRL, true).

% Specification management
-define(REQUEST_NEW_SSG, new_ssg).
-define(REQUEST_GET_SSG, get_ssg).
-define(REQUEST_SET_SSG_NAME, set_ssg_name).
-define(REQUEST_SET_SSG_OWNER, set_ssg_owner).
-define(REQUEST_SET_SSG_EMAIL, set_ssg_email).
-define(REQUEST_SET_SSG_DESCRIPTION, set_ssg_description).
-define(REQUEST_GENERATE, generate).
-define(REQUEST_SET_MODULE_NAME, set_module_name).
-define(REQUEST_USE_MODULE, use_module).
-define(REQUEST_DOWNLOAD_MODULE, download_module).
-define(REQUEST_UPLOAD_MODULE, upload_module).
-define(REQUEST_INSTALL, install).


% Schema management
-define(REQUEST_ADD_SCHEMA, add_schema).
-define(REQUEST_DELETE_SCHEMA, delete_schema).
-define(REQUEST_GET_SCHEMA, get_schema).
-define(REQUEST_GET_ALL_SCHEMAS, get_all_schemas).
-define(REQUEST_SET_SCHEMA_ATTRIBUTES, set_schema_attributes).
-define(REQUEST_GET_SCHEMA_ATTRIBUTE, get_schema_attributes).
-define(REQUEST_GET_SCHEMA_NAMES, get_schema_names).

% Field management
-define(REQUEST_ADD_FIELD, add_field).
-define(REQUEST_MOVE_FIELD, move_field).
-define(REQUEST_MAKE_FIELD_KEY, make_field_key).
-define(REQUEST_SET_FIELD_ATTRIBUTES, set_field_attributes).
-define(REQUEST_GET_FIELD_ATTRIBUTE, get_field_attribute).
-define(REQUEST_GET_FIELDS, get_fields).
-define(REQUEST_GET_FIELD, get_field).
-define(REQUEST_GET_FIELD_COUNT, get_field_count).
-define(REQUEST_GET_MANDATORY_FIELD_COUNT, get_mandatory_field_count).
-define(REQUEST_GET_FIELD_NAMES, get_field_names).
-define(REQUEST_GET_FIELD_POSITION, get_field_position).


% Queries
-define(REQUEST_READ_RECORD, read).
-define(REQUEST_SELECT, select).
-define(REQUEST_SELECT_OR, select_or).
-define(REQUEST_SELECT_AND, select_and).

% Write Operations
-define(REQUEST_ADD_RECORD, add_record).

-endif.