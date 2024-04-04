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
        #panels = soup.find_all('div', class_='col-sm-6')
        panels = soup.find_all('div', class_='panel')
        #print('panels', len(panels), panels)
        all_data = {}

        for panel in panels:
            # make sure we are looking at a leaf panel (no nested panels)
            headings = ['Ownership', 'AdministrativeInformation', 'PropertyValueTotals', 'ValueAdjustmentTotals', 'PropertyType', 'TransferInformation', 'CurrentPropertyValueTotals', 'PreviousPropertyValueTotals', 'CurrentValueAdjustmentTotals', 'PreviousValueAdjustmentTotals', 'Estimated2024TaxUsingEstimatedRevenueNeutralRate','PreviousPropertyTax']
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
                        if key:
                            data[key] = value
                            prev_key = key
                        else:
                            if prev_key:
                                data[prev_key] += '|' + value
                heading = panel.find('div', class_='panel-heading')
                if heading:
                    heading = re.sub('\s','',heading.text)
                    for title in headings:
                        if title in heading:
                            all_data.update({title:data})
                else:
                    print(panel)
            else:
                # Extract "Real Estate ID" value from panel-heading
                pattern = "Real Estate ID:\s+([0-9]+)\s+PIN:\s+(([0-9]+ )+).*\s+Location:\s+([ \w]+)"
                [_,reid,pin,_,location,_] = re.split(pattern,panel.find('div', class_='panel-heading').text)
                all_data.update({'heading':{'reid':reid,'pin':pin,'location':location}})                

        return(all_data)
    else:
        print(f'Failed to download {url}. Status was {response.status_code}')
        return(None)


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
        data = download_extract(url)
        if data:
            # Insert the record into the database
            #cursor.execute('''INSERT INTO tax_records (url, data) VALUES (?, ?)''', (url, str(all_data)))
            #conn.commit()

            # print(f'Tax record {i} downloaded, keys and values extracted, and stored in the database.')
            print(data)
        url = f"{base_revaluation_url}{data['heading']['reid']}"
        revaluation_data = download_extract(url)
        if revaluation_data:
            print(revaluation_data)


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

