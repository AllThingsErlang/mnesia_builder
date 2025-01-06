
-ifndef(DB_API_HRL).
-define(DB_API_HRL, true).

% Specification management
-define(REQUEST_NEW_SSG, new_ssg).
-define(REQUEST_GET_SSG, get_ssg).
-define(REQUEST_SET_SSG_NAME, set_ssg_name).
-define(REQUEST_SET_SSG_OWNER, set_ssg_owner).
-define(REQUEST_SET_SSG_EMAIL, set_ssg_email).
-define(REQUEST_SET_SSG_DESCRIPTION, set_ssg_description).
-define(REQUEST_VALIDATE_SSG, validate_ssg).
-define(REQUEST_GENERATE, generate).
-define(REQUEST_USE_MODULE, use_module).
-define(REQUEST_DOWNLOAD_MODULE, download_module).
-define(REQUEST_UPLOAD_MODULE, upload_module).
-define(REQUEST_INSTALL, install).


% Schema management
-define(REQUEST_ADD_SCHEMA, add_schema).
-define(REQUEST_DELETE_SCHEMA, delete_schema).
-define(REQUEST_GET_SCHEMA, get_schema).
-define(REQUEST_GET_ALL_SCHEMAS, get_all_schemas).
-define(REQUEST_SET_SCHEMA_NAME, set_schema_name).
-define(REQUEST_SET_SCHEMA_TYPE, set_schema_type).

-define(REQUEST_ADD_NODES, add_nodes).  
-define(REQUEST_DELETE_NODES, delete_nodes).

-define(REQUEST_ADD_LOCAL_NODE, add_local_node).
-define(REAUEST_DELETE_LOCAL_NODE, delete_local_node).

-define(REQUEST_ADD_REST_OF_CLUSTER, add_rest_of_cluster).
-define(REQUEST_DELETE_REST_OF_CLUSTER, delete_rest_of_cluster).


-define(REQUEST_GET_SCHEMA_TYPE, get_schema_type).
-define(REQUEST_GET_SCHEMA_RAM_COPIES, get_schema_ram_copies).
-define(REQUEST_GET_SCHEMA_DISC_COPIES, get_schema_ram_copies).
-define(REQUEST_GET_SCHEMA_DISC_ONLY_COPIES, get_schema_ram_copies).

-define(REQUEST_GET_SCHEMA_ATTRIBUTE, get_schema_attributes).
-define(REQUEST_GET_SCHEMA_NAMES, get_schema_names).

% Field management
-define(REQUEST_ADD_FIELD, add_field).

-define(REQUEST_MOVE_FIELD, move_field).
-define(REQUEST_MAKE_FIELD_KEY, make_field_key).

-define(REQUEST_SET_FIELD_TYPE, set_field_type).
-define(REQUEST_SET_FIELD_DESCRIPTION, set_field_description).
-define(REQUEST_SET_FIELD_LABEL, set_field_label).
-define(REQUEST_SET_FIELD_PRIORITY, set_field_priority).
-define(REQUEST_SET_FIELD_DEFAULT_VALUE, set_field_default_value).

-define(REQUEST_GET_FIELD_TYPE, get_field_type).
-define(REQUEST_GET_FIELD_DESCRIPTION, get_field_description).
-define(REQUEST_GET_FIELD_LABEL, get_field_label).
-define(REQUEST_GET_FIELD_PRIORITY, get_field_priority).
-define(REQUEST_GET_FIELD_DEFAULT_VALUE, get_field_default_value).
-define(REQUEST_GET_FIELD_POSITION, get_field_position).

-define(REQUEST_GET_FIELD_ATTRIBUTE, get_field_attribute).
-define(REQUEST_GET_FIELDS, get_fields).
-define(REQUEST_GET_FIELD, get_field).
-define(REQUEST_GET_FIELD_COUNT, get_field_count).
-define(REQUEST_GET_MANDATORY_FIELD_COUNT, get_mandatory_field_count).
-define(REQUEST_GET_FIELD_NAMES, get_field_names).


% Queries
-define(REQUEST_READ_RECORD, read).
-define(REQUEST_SELECT, select).
-define(REQUEST_SELECT_OR, select_or).
-define(REQUEST_SELECT_AND, select_and).

% Write Operations
-define(REQUEST_ADD_RECORD, add_record).

-endif.