import random

import streamlit as st

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Welcome to the NYCSBUS Visualization platform! 👋")

st.sidebar.success("Select a demo above.")
creators = ["Akshay Verma", "Ananya Rajesh", "Dapeng Li"]

st.markdown(
    """
    The NYCSBUS Visualization platform is built by students @
    the CUSP Program in NYU to help match metrics to breakdowns.
    Please look at the sidebar for your pages.
    \n
    Thanks,
    \n
    """
    + (", ").join(random.sample(creators, len(creators)))
)
