import requests
from bs4 import BeautifulSoup
import os
import re
import sqlite3

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
        panel_contents = soup.find_all('div', class_='property-summary-panel-content')

        # Extract keys and values for each section
        all_data = []
        for panel_content in panel_contents:
            data = {}
            prev_key = None
            for row in panel_content.find_all('tr'):
                columns = row.find_all('td')
                if len(columns) == 2:
                    key = columns[0].text.strip()
                    value = columns[1].text.strip()
                    if key:
                        data[key] = value
                        prev_key = key
                    else:
                        if prev_key:
                            data[prev_key] += ' ' + value
            all_data.append(data)

# Base URL
base_url = 'https://services.wake.gov/TaxPortal/Property/Details/'
base_revaluation_url = 'https://services.wake.gov/TaxPortal/TaxCalculator/CalculateTaxes/'
# Function to download tax records, extract keys and values, and insert them into the database
def download_extract_and_store_tax_records(start, end, db_file):
    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create a table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS tax_records (
                    id INTEGER PRIMARY KEY,
                    url TEXT,
                    data TEXT
                    )''')

    # Iterate over the sequential URLs
    for i in range(start, end + 1):
        url = f'{base_url}{i}'
        #print(f'Downloading tax record from {url}...')
        print(i)

        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all property-summary-panel-content sections
            #panels = soup.find_all('div', class_='col-sm-6')
            panels = soup.find_all('div', class_='panel')
            #print('panels', len(panels), panels)
            all_data = {}
            for panel in panels:
                # make sure we are looking at a leaf panel (no nested panels)
                if len(panel.find_all('div', class_='panel')) == 0:
                    heading = panel.find('div', class_='panel-heading')
                    data = {}
                    prev_key = None
                    for row in panel.find_all('tr'):
                        #print ('row:', row)
                        columns = row.find_all('td')
                        #print ('columns:',len(columns), columns)
                        if len(columns) == 2:
                            key = columns[0].text.strip()
                            value = columns[1].text.strip()
                            if key:
                                data[key] = value
                                prev_key = key
                            else:
                                if prev_key:
                                    data[prev_key] += '|' + value
                    all_data.update({heading.text.strip():data})

            # Extract "Real Estate ID" value from panel-heading
            pattern = "Real Estate ID:\s+([0-9]+)\s+PIN:\s+(([0-9]+ )+)\s+Location:\s+([ \w]+)"
            [_,reid,pin,_,location,_] = re.split(pattern,soup.find('div', class_='panel-heading').text)

            # Add REID to the first data dictionary
            #all_data[0]['REID'] = reid
            print(reid,pin,location)
            print(all_data)
            # Insert the record into the database
            #cursor.execute('''INSERT INTO tax_records (url, data) VALUES (?, ?)''', (url, str(all_data)))
            #conn.commit()

            # print(f'Tax record {i} downloaded, keys and values extracted, and stored in the database.')
        else:
            print(f'Failed to download tax record {i}. Status code: {response.status_code}')

    # Close the database connection
    conn.close()
    print('All tax records downloaded, keys and values extracted, and stored in the database.')


# Define the range of tax records to download
start_record = 29993
end_record = 29993  # Adjust as needed

# Define the database file
database_file = 'tax_records.db'

# Download tax records, extract keys and values, and store them in the database
download_extract_and_store_tax_records(start_record, end_record, database_file)

