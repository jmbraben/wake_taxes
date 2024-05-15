import requests
from bs4 import BeautifulSoup
import os
import re
import sqlite3
from multiprocessing import Pool

# Define the database file
database_file = 'tax_records.db'

def download_extract(url,index):
    # Send a GET request to the URL
    try:
        response = requests.get(url, timeout=3)
    except requests.Timeout:
        print(index, url)
        return(None)

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
                        # try and extract number from the data
                        num_test = re.sub('[,\$]','',value)
                        if (re.search('^\s*-*[0-9]+\s*$',num_test)):
                            value = int(num_test)
                        elif (re.search('^\s*-*[0-9][0-9.]+\s*$',num_test)):
                            value = float(num_test)
                        else:
                            #handle special cases of "()" numeric formatting
                            test_pos=re.split('^\$([0-9]+)',re.sub(',','',value))
                            test_neg=re.split('^\(\$([0-9]+)\)',re.sub(',','',value))
                            if(len(test_pos) > 1):
                                value = int(test_pos[1])
                            if(len(test_neg) > 1):
                                value = - int(test_neg[1])

                        if prev_key:
                            data[prev_key] = str(data[prev_key]) + '|' + str(value)
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
                pattern = "Real Estate ID:\s+([0-9]+)\s+PIN:\s+(([0-9]+ )+).*\s+Location:\s+([ \w]+)"
                reid_split =  re.split(pattern,panel.find('div', class_='panel-heading').text)
                if len(reid_split) == 6:
                    [_,reid,pin,_,location,_] = reid_split
                    all_data.update({'heading':{'reid':reid,'pin':pin,'location':location}})                
                else:
                    # Invalid Data
                    return(None)

        return(all_data)
    else:
        return (None)


# Base URL
base_url = 'https://services.wake.gov/TaxPortal/Property/Details/'
base_revaluation_url = 'https://services.wake.gov/TaxPortal/TaxCalculator/CalculateTaxes/'

def download(index):
    property={'index':index, 'summary': None, 'revaluation': None}
    url = f'{base_url}{index}'
    db_data = {
        'id' : index,
        'status' : 'No Data',
        'reid' : '',
        'Owners': '',
        'Location' : '',
        'CorporateLimit' : '',
        'PJ' : '',
        'Zoning' : '',
        'Township' : '',
        'BuildingUse' : '',
        'NBHD' : '',
        'LandClass' : '',
        'DeedDate' : '',
        'SalePrice' : 0,

        'HeatedArea' : 0,
        'Buildings' : 0,
        'OutBuildings' : 0,
        'LandValue' : 0,
        'BuldingValue' : 0,
        'TotalValue' : 0,
        'Exempt' : 0,
        'UseValueDeferred' : 0,
        'HistoricalDeferral' : 0,
        'TaxRelief' : 0,
        'DisabledVeteransExclusion' : 0,
        'TotalAdjustmentValue' : 0,
        'ValueToBeBilled' : 0,
        'PctBilled' : 0,

        'pHeatedArea' : 0,
        'pBuildings' : 0,
        'pOutBuildings' : 0,
        'pLandValue' : 0,
        'pBuldingValue' : 0,
        'pTotalValue' : 0,
        'pExempt' : 0,
        'pUseValueDeferred' : 0,
        'pHistoricalDeferral' : 0,
        'pTaxRelief' : 0,
        'pDisabledVeteransExclusion' : 0,
        'pTotalAdjustmentValue' : 0,
        'pValueToBeBilled' : 0,

        'ChangeInValue' : 0,
    }

    # Connect to SQLite database
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    # Create a table if not exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS tax_records (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    reid TEXT, Owners TEXT, Location TEXT, CorporateLimit TEXT, PJ TEXT, Zoning TEXT, Township TEXT, BuildingUse TEXT, NBHD TEXT, LandClass TEXT, DeedDate TEXT, SalePrice REAL,
                    HeatedArea REAL, Buildings REAL, OutBuildings REAL, LandValue REAL, BuldingValue REAL, TotalValue REAL, Exempt REAL, UseValueDeferred REAL, HistoricalDeferral REAL, TaxRelief REAL, DisabledVeteransExclusion REAL, TotalAdjustmentValue REAL, ValueToBeBilled REAL,
                    PctBilled REAL,
                    pHeatedArea REAL, pBuildings REAL, pOutBuildings REAL, pLandValue REAL, pBuldingValue REAL, pTotalValue REAL, pExempt REAL, pUseValueDeferred REAL, pHistoricalDeferral REAL, pTaxRelief REAL, pDisabledVeteransExclusion REAL, pTotalAdjustmentValue REAL, pValueToBeBilled REAL,
                    ChangeInValue REAL                  
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS raw_data (
                    id INTEGER PRIMARY KEY,
                    raw TEXT
                    )''')    
    property.update({'summary': download_extract(url,index)})
    if (property['summary']):
        db_data.update({
            'status' : 'No Revaluation',
            'reid' : property['summary']['heading']['reid'],
            'Owners': property['summary']['Ownership']['Owners:'],
            'Location' : property['summary']['Ownership']['Location'],
            'CorporateLimit' : property['summary']['AdministrativeInformation']['Corporate Limit:'],
            'PJ' : property['summary']['AdministrativeInformation']['PJ:'],
            'Zoning' : property['summary']['AdministrativeInformation']['Zoning:'],
            'Township' : property['summary']['AdministrativeInformation']['Township:'],
            'BuildingUse' : property['summary']['PropertyType']['Building Type & Use:'],
            'NBHD' : property['summary']['PropertyType']['VCS (NBHD):'],
            'LandClass' : property['summary']['PropertyType']['Land Class:'],
            'DeedDate' : property['summary']['TransferInformation']['Deed Date:'],
            'SalePrice' : property['summary']['TransferInformation']['Pkg Sale Price:'],

            'HeatedArea' : property['summary']['PropertyValueTotals']['Total Heated Area:'],
            'Buildings' : property['summary']['PropertyValueTotals']['Building(s):'],
            'OutBuildings' : property['summary']['PropertyValueTotals']['Outbuilding(s):'],
            'LandValue' : property['summary']['PropertyValueTotals']['Land Value (Assessed):'],
            'BuldingValue' : property['summary']['PropertyValueTotals']['Building Value (Assessed):'],
            'TotalValue' : property['summary']['PropertyValueTotals']['Total Value (Assessed):'],
            'Exempt' : property['summary']['ValueAdjustmentTotals']['Exempt:'],
            'UseValueDeferred' : property['summary']['ValueAdjustmentTotals']['Use Value Deferred:'],
            'HistoricalDeferral' : property['summary']['ValueAdjustmentTotals']['Historical Deferral:'],
            'TaxRelief' : property['summary']['ValueAdjustmentTotals']['Tax Relief:'],
            'DisabledVeteransExclusion' : property['summary']['ValueAdjustmentTotals']['Disabled Veterans Exclusion:'],
            'TotalAdjustmentValue' : property['summary']['ValueAdjustmentTotals']['Total Adjustment Value:'],
            'ValueToBeBilled' : property['summary']['ValueAdjustmentTotals']['Value to be Billed:'],
        })

        if (type(db_data['TotalValue']) not in ['int', 'float']):
            db_data['TotalValue'] = 0
        if (db_data['TotalValue'] > 0):
            # WTH are properties valued at $0?
            db_data.update({'PctBilled': (db_data['ValueToBeBilled']/db_data['TotalValue'])})
        
        url = f"{base_revaluation_url}{property['summary']['heading']['reid']}"
        property.update({'revaluation': download_extract(url,index)})
        if (property['revaluation']):
            db_data.update({
                'status' : 'Complete',
                'pHeatedArea' : property['revaluation']['PreviousPropertyValueTotals']['Total Heated Area:'],
                'pBuildings' : property['revaluation']['PreviousPropertyValueTotals']['Building(s) :'],
                'pOutBuildings' : property['revaluation']['PreviousPropertyValueTotals']['Outbuilding(s) :'],
                'pLandValue' : property['revaluation']['PreviousPropertyValueTotals']['Land Value (Assessed) :'],
                'pBuldingValue' : property['revaluation']['PreviousPropertyValueTotals']['Building Value (Assessed) :'],
                'pTotalValue' : property['revaluation']['PreviousPropertyValueTotals']['Total Value (Assessed) :'],
                'pExempt' : property['revaluation']['PreviousValueAdjustmentTotals']['Exempt:'],
                'pUseValueDeferred' : property['revaluation']['PreviousValueAdjustmentTotals']['Use Value Deferred:'],
                'pHistoricalDeferral' : property['revaluation']['PreviousValueAdjustmentTotals']['Historical Value Deferred:'],
                'pTaxRelief' : property['revaluation']['PreviousValueAdjustmentTotals']['Tax Relief:'],
                'pDisabledVeteransExclusion' : property['revaluation']['PreviousValueAdjustmentTotals']['Disabled Veterans Exclusion:'],
                'pTotalAdjustmentValue' : property['revaluation']['PreviousValueAdjustmentTotals']['Total Adjustment Value:'],
                'pValueToBeBilled' : property['revaluation']['PreviousValueAdjustmentTotals']['Value To Be Billed:'],
            })
            if (type(db_data['pValueToBeBilled']) not in ['int', 'float']):
                db_data['pValueToBeBilled'] = 0

            if (db_data['pValueToBeBilled'] > 0):
                db_data.update({'ChangeInValue': (db_data['ValueToBeBilled']/db_data['pValueToBeBilled'])})

    # Insert the record into the database
    cursor.execute('''INSERT OR REPLACE INTO tax_records VALUES (:id,:status,:reid,:Owners,:Location,:CorporateLimit,:PJ,:Zoning,:Township,:BuildingUse,:NBHD,:LandClass,:DeedDate,:SalePrice,
                   :HeatedArea,:Buildings,:OutBuildings,:LandValue,:BuldingValue,:TotalValue,:Exempt,:UseValueDeferred,:HistoricalDeferral,:TaxRelief,:DisabledVeteransExclusion,:TotalAdjustmentValue,:ValueToBeBilled,:PctBilled,
                   :pHeatedArea,:pBuildings,:pOutBuildings,:pLandValue,:pBuldingValue,:pTotalValue,:pExempt,:pUseValueDeferred,:pHistoricalDeferral,:pTaxRelief,:pDisabledVeteransExclusion,:pTotalAdjustmentValue,:pValueToBeBilled,:ChangeInValue)''', db_data)
    cursor.execute('''INSERT OR REPLACE INTO raw_data (id, raw) VALUES (?, ?)''', (index, str(property)))
    conn.commit()
    conn.close()

    return(property)


# Define the range of tax records to download
# Test Records 29949 (missing valuation), 550000 (no property)
start_record = 539550
end_record = 544999  # Adjust as needed

# retry list
retry=[168650,309837,313288
]

# Download tax records, extract keys and values, and store them in the database

# Iterate over the sequential URLs
#for i in range(start_record, end_record + 1,10):
#    print(i)
#    with Pool(10) as p:
#        data=p.map(download,list(range(i,i+10)))
for i in retry:
    print(download(i))

print('All tax records downloaded, keys and values extracted, and stored in the database.')