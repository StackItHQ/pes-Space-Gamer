# DB Commands

## Create a database
```sql
CREATE DATABASE superjoin;
```

## Create table that stores data
```sql
CREATE TABLE candidates (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(100),
    last_updated TIMESTAMP DEFAULT timezone('utc', NOW())
);
```

## Create a metadata table to store the deleted records
```sql
CREATE TABLE deleted_candidates (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(100),
    last_updated TIMESTAMP DEFAULT timezone('utc', NOW())
);
```

## Create a trigger to move the deleted records to the metadata table
```sql
CREATE OR REPLACE FUNCTION move_deleted_candidates()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO deleted_candidates (first_name, last_name, email, phone)
    VALUES (OLD.first_name, OLD.last_name, OLD.email, OLD.phone);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER move_deleted_candidates
AFTER DELETE ON candidates
FOR EACH ROW
EXECUTE FUNCTION move_deleted_candidates();
```

## Create a trigger to update the last_updated column
```sql
CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = timezone('utc', NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_last_updated
BEFORE UPDATE ON candidates
FOR EACH ROW
EXECUTE FUNCTION update_last_updated();
```