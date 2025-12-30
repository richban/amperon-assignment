-- Generic audit for checking not null constraints
AUDIT (
  name assert_not_null,
  defaults (
    column_name = id
  ),
  blocking TRUE
);
SELECT *
FROM @this_model
WHERE @column_name IS NULL;
