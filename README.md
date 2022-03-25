The uploaded pipeline_json2csv.py script is intended to automate file conversions
from .json to .csv. However, it can also insert data from the .json file into the Mysql database.
By selecting different modes(case = 1 or 2 or 3) in function " json2csv(case)", people can:
1. transform a single .json file into .csv file and save to the same folder(.json filepath is required to input);
2. transform all newly uploaded .json files in the current folder into .csv files and save them in the same folder;
3. extract the data from all newly uploaded .json files in the current folder and insert into tables in Mysql database,
where these tables are created with the same names as .json files.
Currently, the mode is set to be 2.

Author: Renruo Ba
Latest update: 25/03/2022
# interview-homeday
