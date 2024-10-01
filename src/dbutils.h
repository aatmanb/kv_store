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
    class DatabaseUtils {
        public:
            DatabaseUtils(const char *db_name) {
                if (!is_db_open) {
                    file_name = db_name;
                    int retval = sqlite3_open(file_name, &kv_persist_store); 
                    is_db_open = (retval == SQLITE_OK);
		    std::cout << "DB creation process is_db_open: " << is_db_open << " retval: " << retval << "\n";
                    if (!is_db_open) {
                        sqlite3_close(kv_persist_store);
                    } else {
                        // Retry if create table fails?
			    std::cout << "Creating Table\n";
                        int create_status = create_table();
                    }
                }
            }

            std::string put_value (const char *key, const char* value) {
                return put_value_internal(key, value);
            }

            std::string get_value (const char *key) {
                return get_value_internal(key);
            }

            ~DatabaseUtils() {
		std::cout << "DB connection closed prematurely\n";
                sqlite3_close (kv_persist_store);
            }


        private:
            sqlite3 *kv_persist_store;
            const char *file_name;
            bool is_db_open = false;

            int create_table() {
                int retval;
                char *errMsg;

                const char *create_table_sql = "CREATE TABLE IF NOT EXISTS KV_STORE ("\
                                                    "user_key varchar(129) primary key,"\
                                                    "user_value text);";
                retval = sqlite3_exec(kv_persist_store, create_table_sql, 0, 0, &errMsg);
                if (retval != SQLITE_OK) {
                    std::cout << "DB creation failed\n" << errMsg;
                    sqlite3_free(errMsg);
                    return DB_CREATE_FAIL;
                }
                std::cout << "DB creation successful\n";
                return DB_CREATE_SUCCESS;
            }

            std::string get_value_internal (const char *key) {
                sqlite3_stmt *get_stmt;
                const std::string key_not_found = "";
                const char *get_key = "SELECT user_value FROM KV_STORE WHERE user_key = ?;";
                std::cout << __FILE__ << "[" << __LINE__ << "]" << " get_value_interal" << std::endl;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << " key: " << key << std::endl;
                if (sqlite3_prepare_v2(kv_persist_store, get_key, -1, &get_stmt, NULL) != SQLITE_OK) {
                    std::cout << __FILE__ << "[" << __LINE__ << "]" << "Failed to prepare SQL statement: " << sqlite3_errmsg(kv_persist_store) << std::endl;
                }
                
                if (sqlite3_bind_text(get_stmt, 1, key, -1, SQLITE_STATIC) != SQLITE_OK) {
                    std::cout << __FILE__ << "[" << __LINE__ << "]" << "Failed to bind key: " << sqlite3_errmsg(kv_persist_store) << std::endl;
                }
                
                if (sqlite3_step(get_stmt) != SQLITE_ROW) {
                    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "Failed to get key: " << sqlite3_errmsg(kv_persist_store) << std::endl;
		    std::cout << "Get value didn't find any row\n";
		    sqlite3_finalize(get_stmt);
		    return key_not_found;
                }
                
                /* int	retval = sqlite3_prepare_v2(kv_persist_store, get_key, -1, &get_stmt, NULL);
                retval = sqlite3_bind_text(get_stmt, 1, key, -1, SQLITE_STATIC);
                retval = sqlite3_step(get_stmt);
                if (retval != SQLITE_ROW) {
                    std::cout << "Get value didn't find any row\n"; 
                    sqlite3_finalize(get_stmt);
                    return key_not_found;
                } */
                const char *value = reinterpret_cast <const char *> (sqlite3_column_text(get_stmt, 0));
                std::string return_value (value);
                std::cout << "Value returned from get(): " << value << "\n";
                sqlite3_finalize(get_stmt);
                return return_value;
            }

            int insert_value (const char *key, const char *value) {
                sqlite3_stmt *_stmt;
                const char *_key = "INSERT INTO KV_STORE VALUES (?, ?);";
                int retval;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "inserting" << std::endl;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "value: " << value << std::endl;
                if (sqlite3_prepare_v2(kv_persist_store, _key, -1, &_stmt, NULL) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to prepare SQL statement" << std::endl;
                }
                
                if (sqlite3_bind_text(_stmt, 1, key, -1, SQLITE_STATIC) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to bind key" << std::endl;
                }
                
                if (sqlite3_bind_text(_stmt, 2, value, -1, SQLITE_STATIC) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to bind value" << std::endl;
                }
                
                if (sqlite3_step(_stmt) != SQLITE_DONE) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to insert key" << std::endl;
                }
                
                //retval = sqlite3_bind_text(_stmt, 1, key, -1, SQLITE_STATIC);
                //retval = sqlite3_bind_text(_stmt, 2, value, -1, SQLITE_STATIC);
                //retval = sqlite3_step(_stmt); 
                //if (retval != SQLITE_OK) {
                //    std::cout << "Insert value: " << value << " failed for key: " << key << "\n";
                //    sqlite3_finalize(_stmt);
                //    return DB_INSERT_FAIL;
                //}
                std::cout << "Insert value: " << value << " successful for key: " << key << "\n";
                sqlite3_finalize(_stmt);
                return DB_INSERT_SUCCESS;
            }

            int update_value (const char *key, const char *value) {
                sqlite3_stmt *_stmt;
                const char *_key = "UPDATE KV_STORE SET user_value = ? where user_key = ?;";
                int retval;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "updating" << std::endl;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "value: " << value << std::endl;
                if (sqlite3_prepare_v2(kv_persist_store, _key, -1, &_stmt, NULL) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to prepare SQL statement" << std::endl;
                }
                
                if (sqlite3_bind_text(_stmt, 1, value, -1, SQLITE_STATIC) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to bind value" << std::endl;
                }
                
                if (sqlite3_bind_text(_stmt, 2, key, -1, SQLITE_STATIC) != SQLITE_OK) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to bind key" << std::endl;
                }
                
                if (sqlite3_step(_stmt) != SQLITE_DONE) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Failed to update key" << std::endl;
                }
                //int retval = sqlite3_prepare_v2(kv_persist_store, _key, -1, &_stmt, NULL);
                //retval = sqlite3_bind_text(_stmt, 1, value, -1, SQLITE_STATIC);
                //retval = sqlite3_bind_text(_stmt, 2, key, -1, SQLITE_STATIC);
                //retval = sqlite3_step(_stmt); 
                //if (retval != SQLITE_OK) {
                //    std::cout << "Update value: " << value << " failed for key: " << key << "\n";
                //    sqlite3_finalize(_stmt);
                //    return DB_UPDATE_FAIL;
                //}
                std::cout << "Update value: " << value << " successful for key: " << key << "\n";
                sqlite3_finalize(_stmt);
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
    };
}
