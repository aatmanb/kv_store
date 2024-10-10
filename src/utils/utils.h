#ifndef utils_h__
#define utils_h__

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

char* stringToCharArray(const std::string& str);

// Function to convert a modifiable char array to std::string
std::string charArrayToString(char* charArray);

#endif // utils_h__