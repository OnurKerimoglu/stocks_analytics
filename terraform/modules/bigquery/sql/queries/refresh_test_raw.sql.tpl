BEGIN
  DECLARE src_project STRING  DEFAULT '${src_project}';
  DECLARE src_dataset STRING  DEFAULT '${src_dataset}';
  DECLARE dst_project STRING  DEFAULT '${dst_project}';
  DECLARE dst_dataset STRING  DEFAULT '${dst_dataset}';
  DECLARE loc         STRING  DEFAULT '${location}';

  DECLARE prod_tables ARRAY<STRING> DEFAULT [];
  DECLARE dev_tables  ARRAY<STRING> DEFAULT [];
  DECLARE to_drop     ARRAY<STRING> DEFAULT [];

  -- Ensure destination dataset exists
  EXECUTE IMMEDIATE FORMAT("""
    CREATE SCHEMA IF NOT EXISTS `%s.%s`
    OPTIONS (location="%s")
  """, dst_project, dst_dataset, loc);

  -- Collect prod table names
  EXECUTE IMMEDIATE FORMAT("""
    SELECT IFNULL(ARRAY_AGG(table_name), [])
    FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
  """, src_project, src_dataset)
  INTO prod_tables;

  -- Collect dev table names
  EXECUTE IMMEDIATE FORMAT("""
    SELECT IFNULL(ARRAY_AGG(table_name), [])
    FROM `%s.%s`.INFORMATION_SCHEMA.TABLES
    WHERE table_type = 'BASE TABLE'
  """, dst_project, dst_dataset)
  INTO dev_tables;

  -- Clone/refresh all prod tables into dev
  FOR row IN (SELECT table_name FROM UNNEST(prod_tables) AS table_name) DO
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TABLE `%s.%s.%s`
      CLONE `%s.%s.%s`
    """, dst_project, dst_dataset, row.table_name,
         src_project, src_dataset, row.table_name);
  END FOR;

  -- Drop tables that no longer exist in prod
  SET to_drop = (
    SELECT ARRAY(
      SELECT d FROM UNNEST(dev_tables) AS d
      EXCEPT DISTINCT
      SELECT p FROM UNNEST(prod_tables) AS p
    )
  );

  FOR row IN (SELECT table_name FROM UNNEST(to_drop) AS table_name) DO
    EXECUTE IMMEDIATE FORMAT("DROP TABLE `%s.%s.%s`",
                             dst_project, dst_dataset, row.table_name);
  END FOR;
END
