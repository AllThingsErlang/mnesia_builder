{application, mnesia_builder,
 [
  {description, "Mnesia Builder Application"},
  {vsn, "1.0"},
  {modules, [mb_db_edit, mb_db_management, mb_db_query, 
             mb_ipc, mb_schemas, mb_server, mb_supervisor, mb_utilities,
             mb_worker, mnesia_builder]},
  {registered, [mb_supervisor, mb_server]},
  {applications, [kernel, stdlib]},
  {mod, {mnesia_builder, []}}
 ]}.
