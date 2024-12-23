{application, mnesia_builder,
 [
  {description, "Generic Mnesia Application"},
  {vsn, "1.0"},
  {modules, [mnesia_builder, generic_mnesia_sup, 
             schema_modeller_server, schema_modeller_worker, 
             db_server, db_worker]},
  {registered, [schema_modeller, db_access]},
  {applications, [kernel, stdlib]},
  {mod, {mnesia_builder, []}}
 ]}.
