-ifndef(DB_MNESIA_BUILDER_HRL).
-define(DB_MNESIA_BUILDER_HRL, true).



%-------------------------------------------------------------
-define(CURRENT_VERSION, "0.2").

-define(VERSION, version).                      % SSG Attribute
-define(CREATED, created).                      % SSG Attribute
-define(OWNER, owner).                          % SSG Attribute
-define(EMAIL, email).                          % SSG Attribute
-define(SCHEMAS, schemas).                      % SSG Attribute

-define(DISC_COPIES, disc_copies).              % Schema Attribute
-define(DISC_ONLY_COPIES, disc_only_copies).    % Schema Attribute
-define(RAM_COPIES, ram_copies).                % Schema Attribute
-define(SCHEMA_TYPE, type).                     % Schema Attribute
-define(FIELDS, fields).                        % Schema Attribute


-define(LABEL, label).                          % Field Attribute
-define(ROLE, role).                            % Field Attribute
-define(POSITION, position).                    % Field Attribute
-define(PRIORITY, priority).                    % Field Attribute
-define(DEFAULT_VALUE, default_value).          % Field Attribute
-define(FIELD_TYPE, type).                      % Field Attribute

-define(NAME, name).                            % SSG / Schema / Field Attribute
-define(DESCRIPTION, description).              % SSG / Schema / Field Attribute


-define(MNESIA_ROOT_DIR, "../mnesia").
%-------------------------------------------------------------



%-------------------------------------------------------------
-type mb_error() :: {error, term()}.

-type mb_module() :: atom().

-type mb_ssg_attribute() :: ?VERSION | ?NAME | ?DESCRIPTION | ?CREATED | ?OWNER | ?EMAIL | ?SCHEMAS.
-type mb_schema_attribute() :: ?NAME | ?DESCRIPTION | ?SCHEMA_TYPE | ?DISC_COPIES | ?DISC_ONLY_COPIES | ?RAM_COPIES | ?FIELDS.
-type mb_field_attribute() :: ?NAME | ?DESCRIPTION | ?LABEL | ?ROLE | ?POSITION | ?PRIORITY | ?FIELD_TYPE | ?DEFAULT_VALUE.

-type mb_ssg_name() :: atom().

-type mb_schema_name() :: atom().
-type mb_schema_type() :: set | bag | ordered_set.

-type mb_field_name() :: atom().
-type mb_field_role() :: key | field.
-type mb_field_priority() :: mandatory | optional.

-type mb_value_type() :: integer | float | string | list | atom | tuple | map | term.
-type mb_timestamp() :: {{integer(), integer(), integer()}, {integer(), integer(), integer()}}.

-type mb_field_spec() ::  #{?NAME=>mb_field_name(),
                            ?DESCRIPTION=>string(),
                            ?LABEL=>string(),
                            ?ROLE=>mb_field_role(),
                            ?POSITION=>integer(),
                            ?PRIORITY=>mb_field_priority(),
                            ?FIELD_TYPE=>mb_value_type(),
                            ?DEFAULT_VALUE=>term()}.

-type mb_field_spec_list() :: [] | [{mb_field_name(), mb_field_spec()}].

-type mb_schema_spec() :: #{?NAME=>mb_schema_name(),
                            ?DESCRIPTION=>string(),
                            ?SCHEMA_TYPE=>mb_schema_type(),
                            ?DISC_COPIES=>list(),
                            ?DISC_ONLY_COPIES=>list(),
                            ?RAM_COPIES=>list(),
                            ?FIELDS=>mb_field_spec_list()}.

-type mb_schema_spec_list() :: [] | [{mb_schema_name(), mb_schema_spec()}].

-type mb_ssg() :: #{?VERSION=>string(),
                    ?NAME=>mb_ssg_name(),
                    ?DESCRIPTION=>string(),
                    ?CREATED=>mb_timestamp(),
                    ?OWNER=>string(),
                    ?EMAIL=>string(),
                    ?SCHEMAS=>mb_schema_spec_list()}.

-type mb_schema_avp_list() :: [] | [{mb_schema_attribute(), term()}].
-type mb_field_avp_list() :: [] | [{mb_field_attribute(), term()}].
%-------------------------------------------------------------

-endif.