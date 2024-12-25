{application, mnesia_builder,
 [
  {description, "Generic Mnesia Application"},
  {vsn, "1.0"},
  {modules, [mnesia_builder, mb_supervisor, 
             schema_modeller_server, schema_modeller_worker, 
             mb_server, mb_worker]},
  {registered, [schema_modeller, db_access]},
  {applications, [kernel, stdlib]},
  {mod, {mnesia_builder, []}}
 ]}.
