import requests
from bs4 import BeautifulSoup
import os
import re
import sqlite3
from multiprocessing import Pool

# Create a directory to save downloaded HTML files
if not os.path.exists('tax_records'):
    os.makedirs('tax_records')

def download_extract(url):
    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all property-summary-panel-content sections
        panels = soup.find_all('div', class_='panel')
        if not panels:
            return (None)
        #print('panels', len(panels), panels)
        all_data = {}

        for panel in panels:
            # make sure we are looking at a leaf panel (no nested panels)
            headings = ['Ownership', 'AdministrativeInformation', 'PropertyValueTotals', 'ValueAdjustmentTotals', 'PropertyType', 'TransferInformation', 'CurrentPropertyValueTotals', 'PreviousPropertyValueTotals', 'CurrentValueAdjustmentTotals', 'PreviousValueAdjustmentTotals', 'Estimated2024TaxUsingEstimatedRevenueNeutralRate','PreviousPropertyTax']
            split_keys = ['Owners:', 'Location', 'Mailing']
            if len(panel.find_all('div', class_='panel')) == 0:
                data = {}
                prev_key = None
                for row in panel.find_all('tr'):
                    #print ('row:', row)
                    columns = row.find_all('td')
                    #print ('columns:',len(columns), columns)
                    if len(columns) >= 2:
                        key = columns[0].text.strip()
                        value = columns[1].text.strip()
                        num_test = re.sub('[,\$]','',value)
                        if (re.search('^\s*-*[0-9]+\s*$',num_test)):
                            value = int(num_test)
                        elif (re.search('^\s*-*[0-9.]+\s*$',num_test)):
                            value = float(num_test)
                        if prev_key:
                            data[prev_key] += '|' + value
                            prev_key = None
                        elif key not in split_keys:
                            data[key] = value
                        else:
                            data[key] = value
                            prev_key = key
                    else:
                        # reset previous key in case no data in next record
                        prev_key = None
                heading = panel.find('div', class_='panel-heading')
                if heading:
                    heading = re.sub('\s','',heading.text)
                    for title in headings:
                        if (re.search(f'^{title}',heading)):
                            all_data.update({title:data})
                #else:
                    # print(panel)
            else:
                # Extract "Real Estate ID" value from panel-heading
                reid = ''
                pin = ''
                location = ''
                pattern = "Real Estate ID:\s+([0-9]+)\s+PIN:\s+(([0-9]+ )+).*\s+Location:\s+([ \w]+)"
                reid_split =  re.split(pattern,panel.find('div', class_='panel-heading').text)
                if len(reid_split) == 6:
                    [_,reid,pin,_,location,_] = reid_split
                all_data.update({'heading':{'reid':reid,'pin':pin,'location':location}})                

        return(all_data)
    else:
        print(f'Failed to download {url}. Status was {response.status_code}')
        return (None)


# Base URL
base_url = 'https://services.wake.gov/TaxPortal/Property/Details/'
base_revaluation_url = 'https://services.wake.gov/TaxPortal/TaxCalculator/CalculateTaxes/'

def download(index):
    property={'index':index, 'summary': None, 'revaluation': None}
    url = f'{base_url}{index}'
    property.update({'summary': download_extract(url)})
    # Connect to SQLite database
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    # Create a table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS tax_records (
                    id INTEGER PRIMARY KEY,
                    reid TEXT,
                    data TEXT
                    )''')
    
    if (property['summary']):
        url = f"{base_revaluation_url}{property['summary']['heading']['reid']}"
        property.update({'revaluation': download_extract(url)})

    db_data = {
        'index' : index,
        'reid' : property['summary']['heading']['reid'],
        'Owners': property['summary']['Ownership']['Owners:'],
        'Location' : property['summary']['Ownership']['Location'],
        'CorporateLimit' : property['summary']['AdministrativeInformation']['Corporate Limit:'],
        'PJ' : property['summary']['AdministrativeInformation']['PJ:'],
        'Zoning' : property['summary']['AdministrativeInformation']['Zoning:'],
        'Township' : property['summary']['AdministrativeInformation']['Township:'],
        'HeatedArea' : property['summary']['PropertyValueTotals']['Total Heated Area:'],
        'Buildings' : property['summary']['PropertyValueTotals']['Building(s):'],
        'OutBuildings' : property['summary']['PropertyValueTotals']['Outbuilding(s):'],
    }

    # Insert the record into the database
    cursor.execute('''INSERT OR REPLACE INTO tax_records (id, reid, data) VALUES (?, ?, ?)''', (index, property['summary']['heading']['reid'], str(property)))
    conn.commit()
    conn.close()

    return(property)

# Function to download tax records, extract keys and values, and insert them into the database
def download_extract_and_store_tax_records(start, end, db_file):


    # Iterate over the sequential URLs
    #for i in range(start, end + 1,10):
    #    with Pool(10) as p:
    #        print(p.map(download,list(range(i,i+10))))
    for i in range(start, end + 1):
        print(download(i))


    # Close the database connection
    print('All tax records downloaded, keys and values extracted, and stored in the database.')


# Define the range of tax records to download
start_record = 29993
end_record = 29993  # Adjust as needed

# Define the database file
database_file = 'tax_records.db'

# Download tax records, extract keys and values, and store them in the database
download_extract_and_store_tax_records(start_record, end_record, database_file)

