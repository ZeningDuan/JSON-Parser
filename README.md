# Raw Tweet JSON Parsers

This repository will provide parsers for resolving two types of raw JSONs:
1. **Standard JSONs** collected directly from Twitter APIs (e.g., Firehose)
2. **Rrhydrated JSONs** collected from tweet rehydration by twitter ids. 

Parser for rehydrated JSONs is available now. Parser for standard JSONs will be available soon. 



<!-- TABLE OF CONTENTS -->
<summary><h2 style="display: inline-block">Table of Contents</h2></summary>
<details open="open">  
  <ol>
    <li><a href="#about-the-parser">About the Parser</a>
    <li><a href="#getting-started">Getting Started</a>
    <li><a href="#prerequisites">Prerequisites</a></li>
    <li><a href="#installation">Installation</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About the Parser

This Parser was built in Python 3.7. 

Who would benefit from this tool?
* If you have a bunch of raw tweet JSONs in hand. 
* If you plan to construct the original tweets and extract key information based on the raw JSONs.
* If you plan to filter raw JSONs by language and/or keywords to narrow down the scope.
* If you plan an accurate, stable, and replicable framework to analyze raw tweet JSONs.

**New updates and versions are comming soon.** In late Feb, I will share parsers applying for standard tweet JSONs.



<!-- GETTING STARTED -->
## Getting Started
It is hardly to have an one-for-all script to parse your data as the interested tags/fields/info vary. Modifications are always necessary and encouraged. For any question, please check the contact info below.


<!-- PREREQUISITIES -->
### Prerequisites
1. You will need basic Python programming skills, specifically, experiences working with [dictionary](https://realpython.com/python-dicts/), calling a packed [function](http://introtopython.org/introducing_functions.html), and interacting with [pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html).

2. Python packages:
  - pathlib
  - csv
  - json
  - jsonlines
  - tqdm
  - os

<!-- INSTALLATION -->
### Installation
1. Download [Python Jupyter](https://jupyter.org/install)

<!-- CONTACT -->
### Contact
Zening 'Ze' Duan, [_SJMC_](https://journalism.wisc.edu/), University of Wisconsin-Madison

zening.duan AT wisc DOT edu 

<!-- ACKNOWLEDGEMENTS -->
### Acknowledgements
Thanks Yachao Qian for help in planning and code testing, and thank Sijia and Dhavan for their support.

