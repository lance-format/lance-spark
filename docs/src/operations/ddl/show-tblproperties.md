# SHOW TBLPROPERTIES

Display the properties stored on a Lance table.

## Show All Properties

```sql
SHOW TBLPROPERTIES users;
```

Returns all key-value properties as a two-column result set (`key`, `value`).

## Show a Specific Property

```sql
SHOW TBLPROPERTIES users ('team');
```

Returns the value of the specified property key, or an informational message if the key does not exist.
