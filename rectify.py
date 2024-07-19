# INTERNAL LIBRARY TO RECTIFY DATABASE WHEN NEEDED
import os
curr_dir = os.path.dirname(os.path.abspath(__file__))

# loading environment variables
from dotenv import load_dotenv

env_path = f"{curr_dir}/.env"
load_dotenv(env_path)

from shared import tools, config, userops
tools.initialize(config.get_db_connection_str(), config.get_embedder_model_path(), config.get_llm_api_key())


EDITOR_USER = "__EDITOR__"

EDITORS_CHOICE = {
    "Business & Finance": "IPO, Investing, Federal Trade Commission, Tech Company, Fiscal Reports, Stock Market, Company CEO, CTO, CXO related contents, Fiance, Economics, Mergers & Acquisitions, Company shutdown, Bankcrupcy",
    "Crypto": "Crypto Currency, Crypto Finance, Crypto Currency Technology and Tools, Ethereum, Bitcoin, Dogecoin, Solidity Framework, Digital Currency, Crypto Currency Company. Crypto Currency Policy & Regulations",
    "Cyber Security": "Cyber Security, Cyber-crime, Information Security, Cyber Threat Intelligence, Data Breach, Network Breach, Cyber Security Tool, Cybersecurity Incidents, Cyber Security Events, Cyber Security Policty & Regulations related content",
    "Electronics & Gadgets": "New Laptop Release, New Phone Release, Windows, Linux, MacOS, Android, iPhone, New Device Release, New Headphone Release, New Wearables related content",
    "Generative AI": "Generative AI, Large Language Models, Use of AI in Enterprise, Text-to-Vision Models, Text-to-Speech Models, Machine Learning, MLOps, AI Tools, AI Policy & Regulations, AI Safety related content",
    "HPC & Hardware": "High Performance Computing, Hardware, CPU, GPU, LPU, Register, RAM, Chips, Supercomputer, Datacenter",
    "Influencial Figures": "Elon Musk, Satya Nadala, Sam Altman, Sundar Pichai, Jeff Bezos, Microsoft, Google, Apple, Netflix, Amazon, OpenAI, Twitter, SpaceX, Tesla, US President, Tech CEO related content",
    "Politics": "US Politics, International Politics, Ongoing War, Immigration and Border Policy, Election, Congress, Parliament, Senate related content",
    "Robotics": "Robots, Autonomous Vehicles, Use of Generative AI in Robots, New Robotics Technology, Industrial Robots, Home-use Robots, Everyday Utility Robots related content", 
    "Space & Rockets": "SpaceX, NASA, Starship, Rocket Launch, Satelites, Space Technology related coontent",    
    "Start-ups": "Tech Start-ups, Start-up Fund Raising, Start-up Lessons, Tech Start-up Launch related content"
}

userops.update_topics(EDITOR_USER, EDITORS_CHOICE)