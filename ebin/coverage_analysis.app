{application, coverage_analysis,
 [
  {description, "Project for managing the data for core wireless coverage issues"},
  {vsn, "1.0.0"},
  {modules, [coverage, coverage_sup, setup_db]},
  {applications, [kernel, stdlib, mnesia]}
 ]}.

