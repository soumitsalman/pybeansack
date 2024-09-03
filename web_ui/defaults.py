from pybeansack import datamodels

SAVE_DELAY = 60
# search settings
DEFAULT_WINDOW = 7
MIN_WINDOW = 1
MAX_WINDOW = 30
DEFAULT_LIMIT = 10
MIN_LIMIT = 1
MAX_LIMIT = 100
MAX_ITEMS_PER_PAGE = 5
MAX_PAGES = 10
MAX_TAGS_PER_BEAN = 3
MAX_RELATED_ITEMS = 5

NAVIGATION_HELP = "Click on 📈 and 🔥 buttons for more trending stories by topics. \n\nClick on ⚙️ button to change topics and time window."
EXAMPLE_OPTIONS = ["Earnings report", "trending -t posts -q \"cyber security breches\"", "search -q \"GPU vs LPU\"", "settings -d 7 -n 20"]   
PLACEHOLDER = "Tell me lies, sweet little lies"

# themes
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:wght@400;700&display=swap');
    
body {
    font-family: 'Open Sans', sans-serif;
    color: #1D1D1D;        
}

.text-caption { color: gray; }
"""

SECONDARY_COLOR = "#ADD8E6"