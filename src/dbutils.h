#pragma once

#include <cstring>
#include <iostream>
#include "singleton.h"
#include "sqlite3/sqlite3.h"

#define DB_CREATE_FAIL 0
#define DB_CREATE_SUCCESS 1

#define DB_INSERT_FAIL 0
#define DB_INSERT_SUCCESS 1

#define DB_UPDATE_FAIL 0
#define DB_UPDATE_SUCCESS 1

namespace key_value_store {
    class DatabaseUtils: public Singleton<DatabaseUtils> {
        friend class Singleton<DatabaseUtils>;

        public:
            static DatabaseUtils& get_instance(const char *db_name) {
                static DatabaseUtils dbUtils(db_name);
                return dbUtils;
            }

            std::string put_value (const char *key, const char* value) {
                return put_value_internal(key, value);
            }

            std::string get_value (const char *key) {
                return get_value_internal(key);
            }

        private:
            sqlite3 *kv_persist_store;
            const char *file_name;
            bool is_db_open = false;

            DatabaseUtils(const char *db_name) {
                if (!is_db_open) {
                    file_name = db_name;
                    int retval = sqlite3_open(file_name, &kv_persist_store); 
                    is_db_open = (retval == SQLITE_OK);
                    if (!is_db_open) {
                        sqlite3_close(kv_persist_store);
                    } else {
                        // Retry if create table fails?
                        int create_status = create_table();
                    }
                }
            }

            int create_table() {
                int retval;
                char *errMsg;

                const char *create_table_sql = "CREATE TABLE IF NOT EXISTS KV_STORE ("\
                                                    "user_key varchar(129) primary key,"\
                                                    "user_value text);";
                retval = sqlite3_exec(kv_persist_store, create_table_sql, 0, 0, &errMsg);
                if (retval != SQLITE_OK) {
                    std::cout << "DB creation failed\n";
                    sqlite3_free(errMsg);
                    return DB_CREATE_FAIL;
                }
                std::cout << "DB creation successful\n";
                return DB_CREATE_SUCCESS;
            }

            std::string get_value_internal (const char *key) {
                sqlite3_stmt *get_stmt;
                const std::string key_not_found = "";
                const char *get_key = "SELECT user_value FROM KV_STORE where user_key = ?;";
                int	retval = sqlite3_prepare_v2(kv_persist_store, get_key, -1, &get_stmt, NULL);
                retval = sqlite3_bind_text(get_stmt, 1, key, -1, SQLITE_STATIC);
                retval = sqlite3_step(get_stmt);
                if (retval != SQLITE_ROW) {
                    std::cout << "Get value didn't find any row\n"; 
                    sqlite3_finalize(get_stmt);
                    return key_not_found;
                } 
                const char *value = reinterpret_cast <const char *> (sqlite3_column_text(get_stmt, 0));
                std::string return_value (value);
                std::cout << "Value returned from get(): " << value << "\n";
                sqlite3_finalize(get_stmt);
                return return_value;
            }

            int insert_value (const char *key, const char *value) {
                sqlite3_stmt *insert_stmt;
                const char *insert_key = "INSERT INTO KV_STORE VALUES (?, ?);";
                int retval = sqlite3_prepare_v2(kv_persist_store, insert_key, -1, &insert_stmt, NULL);
                retval = sqlite3_bind_text(insert_stmt, 1, key, -1, SQLITE_STATIC);
                retval = sqlite3_bind_text(insert_stmt, 2, value, -1, SQLITE_STATIC);
                retval = sqlite3_step(insert_stmt); 
                if (retval != SQLITE_DONE) {
                    std::cout << "Insert value: " << value << " failed for key: " << key << "\n";
                    sqlite3_finalize(insert_stmt);
                    return DB_INSERT_FAIL;
                }
                std::cout << "Insert value: " << value << " successful for key: " << key << "\n";
                sqlite3_finalize(insert_stmt);
                return DB_INSERT_SUCCESS;
            }

            int update_value (const char *key, const char *value) {
                sqlite3_stmt *update_stmt;
                const char *update_key = "UPDATE KV_STORE SET user_value = ? where user_key = ?;";
                int retval = sqlite3_prepare_v2(kv_persist_store, update_key, -1, &update_stmt, NULL);
                retval = sqlite3_bind_text(update_stmt, 1, value, -1, SQLITE_STATIC);
                retval = sqlite3_bind_text(update_stmt, 2, key, -1, SQLITE_STATIC);
                retval = sqlite3_step(update_stmt); 
                if (retval != SQLITE_DONE) {
                    std::cout << "Update value: " << value << " failed for key: " << key << "\n";
                    sqlite3_finalize(update_stmt);
                    return DB_UPDATE_FAIL;
                }
                std::cout << "Update value: " << value << " successful for key: " << key << "\n";
                sqlite3_finalize(update_stmt);
                return DB_UPDATE_SUCCESS;                
            }

            std::string put_value_internal (const char *key, const char *value) {
                std::string old_value = get_value_internal (key);
                int retval;

                // Check if old_value is not empty (i.e key is not found)
                if (old_value == "") {
                    retval = insert_value (key, value);        
                } else {
                    retval = update_value (key, value);
                }
                return old_value;
            }

            ~DatabaseUtils() {
                sqlite3_close (kv_persist_store);
            }
    };
}
