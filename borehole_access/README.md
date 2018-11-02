# Borehole Access

UNHCR has the primary responsibility for coordinating, drafting, updating and promoting guidance related to water, sanitation and hygiene (WASH) in refugee settings.

We were for the first time able to collect extensive geolocation data for a number of African refugee camps, down to individual tents. In addition, UNHCR WASH provides borehole data with detailed information about the properties, location and safe yield of the boreholes that provide water supply to millions of refugees.

Water access is a basic human right and is essential to the life, health, and dignity of refugee populations. Using UNHCR WASH standards as guidance, the borehole and geolocation data can be combined to provide effective visualizations that illustrate the quality of water access. UNHCR WASH already provides a basic exploration tool (http://wash.unhcr.org/wash-gis-portal) for the borehole data. Map overlays might be created that illustrate how well populations are currently served in terms of the safe supply of water provided by boreholes within some spatial distance. 

Some questions to explore include: Where are populations most at risk in case a pump fails, and in what locations would new boreholes make the biggest difference to people? Are there ways of connecting the experience of refugees to people in other countries, that might help underline the hardship in camps (i.e., until where would someone have to walk from their house in California if they had to rely on boreholes for water access?). 

The tools you build, and the insights you glean, can both help UNHCR make a difference on the ground and raise awareness for the refugee crisis.

## UNHCR WASH Resources
http://wash.unhcr.org/

### Emergency Water Standard
For UNHCR camp standards for water access, see here: https://emergency.unhcr.org/entry/248763/emergency-water-standard
For UNHCR camp standard for planned camps, see here: https://emergency.unhcr.org/entry/248797/camp-planning-standards-planned-settlements

### WASH Manual
http://wash.unhcr.org/unhcr-wash-manual-for-refugee-settings/


## Datasets

### UNHCR WASH Borehole Data
A csv file can be downloaded here: http://wash.unhcr.org/wash-gis-portal/. It is also in the `tools_borehole_data` folder as`boreholes_wash.csv`. Note the data is sparse and not all boreholes that overlap with mapped tents include `yield` or `daily pumping time`.

### Refugee Camp Geolocation Data
Contained in the `UNHCR_data` subfolder. Note that camp and borehole datasets do not perfectly overlap, but you can view the list of camps and boreholes that overlap (`tools_borehole_data`)  - all of which are in Africa. 

### General Purpose Satellite Imagery
GBDX (https://platform.digitalglobe.com/gbdx/) provides vast amounts of satelite imagery. You will have access to this imagery by signing up for a free 30 day account with GBDX notebooks (https://notebooks.geobigdata.io/) where you can also access tutorials on how to use the platform or see some existing work on refugee tent mapping (https://notebooks.geobigdata.io/hub/tutorials/5b48ee812486966ea89b7657?tab=code) 
