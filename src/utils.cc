#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

// Function to convert std::string to modifiable char*
char* stringToCharArray(const std::string& str) {
    // Allocate memory for the char array (+1 for the null terminator)
    char* charArray = new char[str.length() + 1];
    
    // Copy the contents of the string into the char array
    std::strcpy(charArray, str.c_str());
    
    // Return the modifiable char array
    return charArray;
}

// Function to convert a modifiable char array to std::string
std::string charArrayToString(char* charArray) {
    // Create a std::string from the char array
    return std::string(charArray);
}

