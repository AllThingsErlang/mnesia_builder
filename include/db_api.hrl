
-ifndef(DB_API_HRL).
-define(DB_API_HRL, true).

% Specification management
-define(REQUEST_NEW_SPECIFICATIONS, new_specifications).
-define(REQUEST_GENERATE, generate).
-define(REQUEST_GET_SPECIFICATIONS, get_specifications).

% Schema management
-define(REQUEST_ADD_SCHEMA, add_schema).
-define(REQUEST_DELETE_SCHEMA, delete_schema).
-define(REQUEST_GET_SCHEMA, get_schema).
-define(REQUEST_SET_SCHEMA_ATTRIBUTES, set_schema_attributes).
-define(REQUEST_GET_SCHEMA_ATTRIBUTE, get_schema_attributes).
-define(REQUEST_GET_SCHEMA_NAMES, get_schema_names).

% Field management
-define(REQUEST_ADD_FIELD, add_field).
-define(REQUEST_SET_FIELD_ATTRIBUTES, set_field_attributes).
-define(REQUEST_GET_FIELD_ATTRIBUTE, get_field_attribute).
-define(REQUEST_GET_FIELDS, get_fields).
-define(REQUEST_GET_FIELD, get_field).
-define(REQUEST_GET_FIELD_COUNT, get_field_count).
-define(REQUEST_GET_MANDATORY_FIELD_COUNT, get_mandatory_field_count).
-define(REQUEST_GET_FIELD_NAMES, get_field_names).
-define(REQUEST_GET_FIELD_POSITION, get_field_position).

-endif.