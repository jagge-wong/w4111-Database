
CSV:
find_by_template£ºiterate through all rows, and find those in match with the template. Using a list to store those rows.
find_by_primary_key: match the key columns with inserted values and make them a dictionary. Use the dictionary as a template and find the result with find_by_template 
delete_by_key: use find_by_primary_key, and then use remove method to delete.
delete_by_template: iterate all rows, and matched ones. Use remove method to delete.
update_by_key: use find_by_key, and use update method to update. Make sure the data is valid. 
update_by_template: use find_by_template, and use update method to update.
insert: make sure that inserted data is valid, and use add_rows method.

-------------------------------------------------------

RDB:
find_by_template£ºuse helper method select, and run_q
find_by_primary_key: match the key columns with values, use helper method select, and run_q
delete_by_key: use find_by_key, and then use helper method delete, and run_q
delete_by_template: use find_by_template, and use helper method delete, and run_q
update_by_key: use find_by_key, and use helper method update
update_by_template: use find_by_template, and use helper method update
insert: use create_insert helper method
