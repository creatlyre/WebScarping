import streamlit as st
import os
import sys

# Add the current directory to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

import budget_checker

def main():
    st.title("Future Price calculator")
    result = budget_checker.main()  # Assuming your script has a main function
    st.write(result)

if __name__ == "__main__":
    main()
