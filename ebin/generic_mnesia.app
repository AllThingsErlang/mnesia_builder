{application, generic_mnesia,
 [
  {description, "Generic Mnesia Application"},
  {vsn, "1.0"},
  {modules, [generic_mnesia, generic_mnesia_sup, 
             schema_modeller_server, schema_modeller_worker, 
             db_server, db_worker]},
  {registered, [schema_modeller, db_access]},
  {applications, [kernel, stdlib]},
  {mod, {generic_mnesia, []}}
 ]}.
