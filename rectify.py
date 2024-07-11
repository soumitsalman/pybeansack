# INTERNAL LIBRARY TO RECTIFY DATABASE WHEN NEEDED

from shared import userops

# TODO: convert this to python
# EDITOR_USER = "__EDITOR__"

# // Create a new document in the collection.
# db.getCollection('preferences').insertOne({
#     _id: EDITOR_USER
# });


EDITORS_CHOICE = {
    "Business & Finance": "IPO, Federal Trade Commission, Tech Company, Fiscal Reports, Stock Market, Company CEO, CTO, CXO related contents",
    "Crypto": "Crypto Currency, Crypto Finance, Crypto Currency Technology and Tools, Ethereum, Bitcoin, Dogecoin, Solidity Framework, Digital Currency, Crypto Currency Company. Crypto Currency Policy & Regulations",
    "Cyber Security": "Cyber Security, Information Security, Cyber Threat Intelligence, Data Breach, Network Breach, Cyber Security Tool, Cybersecurity Incidents, Cyber Security Events, Cyber Security Policty & Regulations related content",
    "Electronics & Gadgets": "New Laptop Release, New Phone Release, Windows, Linux, MacOS, Android, iPhone, New Device Release, New Headphone Release, New Wearables related content",
    "Generative AI": "Generative AI, Large Language Models, Use of AI in Enterprise, Text-to-Vision Models, Text-to-Speech Models, Machine Learning, MLOps, AI Tools, AI Policy & Regulations, AI Safety related content",
    "Influencial Figures": "Elon Musk, Satya Nadala, Sam Altman, Sundar Pichai, Jeff Bezos, Microsoft, Google, Apple, Netflix, Amazon, OpenAI, Twitter, SpaceX, Tesla, US President, Tech CEO related content",
    "Politics": "US Politics, International Politics, Ongoing War, Immigration and Border Policy, Election, Congress, Parliament, Senate related content",
    "Robotics": "Robots, Autonomous Vehicles, Use of Generative AI in Robots, New Robotics Technology, Industrial Robots, Home-use Robots, Everyday Utility Robots related content", 
    "Space & Rockets": "SpaceX, NASA, Starship, Rocket Launch, Satelites, Space Technology related coontent",    
    "Start-ups": "Tech Start-ups, Start-up Fund Raising, Start-up Lessons, Tech Start-up Launch related content"
}

userops.update_preferences(EDITORS_CHOICE)