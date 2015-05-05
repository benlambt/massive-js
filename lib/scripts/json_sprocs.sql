CREATE EXTENSION IF NOT EXISTS plcoffee;

CREATE or REPLACE FUNCTION json_set(collec TEXT, id TEXT, path TEXT, value JSON)
RETURNS JSON
AS $$

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts[...-1]
    cursor = cursor[part]
  cursor[parts[parts.length-1]] = value

  sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
    ['jsonb', 'text']
  sql.execute [JSON.stringify(data), id]
  sql.free()

  data

$$ LANGUAGE plcoffee;


CREATE or REPLACE FUNCTION json_insert(collec TEXT, id TEXT, path TEXT, value JSON)
RETURNS JSON
AS $$

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts[...-1]
    cursor = cursor[part]
  index = parts[parts.length-1]
  cursor.splice index, 0, value

  sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
    ['jsonb', 'text']
  sql.execute [JSON.stringify(data), id]
  sql.free()

  return data

$$ LANGUAGE plcoffee;


CREATE or REPLACE FUNCTION json_push(collec TEXT, id TEXT, path TEXT, value JSON)
RETURNS JSON
AS $$

  # Pushes values to arrays only

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts
    cursor = cursor[part]
  cursor.push value

  sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
    ['jsonb', 'text']
  sql.execute [JSON.stringify(data), id]
  sql.free()

  return data

$$ LANGUAGE plcoffee;


CREATE or REPLACE FUNCTION json_push_uniq(collec TEXT, id TEXT, path TEXT, value JSON)
RETURNS JSON
AS $$

  # Pushes values to arrays only

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts
    cursor = cursor[part]

  alreadyPresent = false
  for item in cursor
    if item is value
      alreadyPresent = true
      break

  unless alreadyPresent
    cursor.push value

    sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
      ['jsonb', 'text']
    sql.execute [JSON.stringify(data), id]
    sql.free()

  return data

$$ LANGUAGE plcoffee;


CREATE or REPLACE FUNCTION json_remove(collec TEXT, id TEXT, path TEXT)
RETURNS JSON
AS $$

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts[...-1]
    cursor = cursor[part]
  key = parts[parts.length-1]
  if isNaN(key)
    delete cursor[key]
  else
    cursor.splice key, 1


  sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
    ['jsonb', 'text']
  sql.execute [JSON.stringify(data), id]
  sql.free()

  return data

$$ LANGUAGE plcoffee;


CREATE or REPLACE FUNCTION json_remove_val(collec TEXT, id TEXT, path TEXT, value JSON)
RETURNS JSON
AS $$

  # Removes values from arrays only

  sql = plv8.prepare "select body::json from #{collec} where id = $1 for update
    ", ['text']
  rows = sql.execute [id]
  data = rows[0].body
  sql.free()

  parts = path.split '.'
  cursor = data
  for part in parts
    cursor = cursor[part]

  index = -1
  for item, i in cursor
    if item is value
      index = i
      break

  if index >= 0
    cursor.splice index, 1

    sql = plv8.prepare "update #{collec} set body = $1 where id = $2",
      ['jsonb', 'text']
    sql.execute [JSON.stringify(data), id]
    sql.free()

  return data

$$ LANGUAGE plcoffee;
