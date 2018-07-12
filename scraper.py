'''
scraper.py
-----------

Scrapes the following data about every GDQ event from the donation tracker:
* donations
* runs
* prizes
* bids/donation incentives
'''
import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from sys import argv

# root url for GDQ donation tracker
root = 'https://gamesdonequick.com/tracker/{}'


def scrape_donations():
    # dictionary of columns and column names
    donations = {'Name': [], 'Timestamp': [], 'Amount': [], 'Comment': []}

    # scrape each page, with a delay in between
    for i in range(1, 7529):
        # load the page into a BeautifulSoup object
        soup = BeautifulSoup(requests.get(root.format('/donations/?page={}'.format(i)), 'r').text, 'html.parser')
        donation_index = soup.find_all('tr')[1:]

        # concatenate the columns from each page onto the existing columns
        donations['Name'] += [clean_text(tr.find_all('td')[0].get_text()) for tr in donation_index]
        donations['Timestamp'] += [clean_text(tr.find_all('td')[1].get_text()) for tr in donation_index]
        donations['Amount'] += [clean_text(tr.find_all('td')[2].get_text()) for tr in donation_index]
        donations['Comment'] += [clean_text(tr.find_all('td')[3].get_text()) for tr in donation_index]

        # print a status update every 10 pages
        if i % 10 == 0:
            print('{} out of 7528 pages scraped!'.format(i))

        # delay scraping the next page
        time.sleep(5)

    # write the data into a csv file
    write_csv(pd.DataFrame.from_dict(donations), 'donations.csv')


def scrape_runs():
    # load the run index into a BeautifulSoup object
    soup = BeautifulSoup(requests.get(root.format('runs/'), 'r').text, 'html.parser')
    run_index = soup.find_all('tr')
    colnames = []
    columns = []

    # extract columns and column names
    for i in range(6):
        # remove commas and newlines from text
        columns.append([clean_text(tr.find_all('td')[i].get_text()) for tr in run_index[1:]])
        colnames.append(clean_text(run_index[0].find_all('th')[i].get_text()))

        # for the run name column, also extract the run id from the link in the tag
        if i == 0:
            columns.append([tr.find('a').attrs['href'].replace('/tracker/run/', '') for tr in run_index[1:]])
            colnames.append('run_id')
    
    # write data to a csv file
    write_csv(pd.DataFrame.from_dict({k:v for (k, v) in zip(colnames, columns)}), 'runs.csv')


def scrape_prizes():
    # load the prize index into a BeautifulSoup object
    soup = BeautifulSoup(requests.get(root.format('prizes/'), 'r').text, 'html.parser')
    prize_index = soup.find_all('tr')
    colnames = []
    columns = []

    # extract colums and column names
    for i in range(7):
        # for the games column, extract the start/end run ids instead
        if i == 3:
            # extracting the game column allows us to elegantly extract the start and end run ids
            ids = [tr.find_all('td')[3].find_all('a') for tr in prize_index[1:]]
            
            columns.append([a[0].attrs['href'].replace('/tracker/run/', '') if len(a) > 0 else 'Beginning of marathon' for a in ids])
            colnames.append('start_id')

            columns.append([a[-1].attrs['href'].replace('/tracker/run/', '') if len(a) > 0 else 'End of marathon' for a in ids])
            colnames.append('end_id')
        else:
            columns.append([clean_text(tr.find_all('td')[i].get_text()) for tr in prize_index[1:]])
            colnames.append(clean_text(prize_index[0].find_all('th')[i].get_text()))
    
    # write data to a csv file
    write_csv(pd.DataFrame.from_dict({k:v for (k, v) in zip(colnames, columns)}), 'prizes.csv')


def scrape_bids():
    # set the list of all events that have a nonempty bid index
    events = ['sgdq2011', 'hrdq'] + [s + y for s in ['agdq', 'sgdq'] for y in [str(i) for i in range(2012, 2019)]]
    # dictionary of columns and column names
    bids = {'Bid Name': [], 'Bid Choice': [], 'Event': [], 'Run': [], 'Description': [], 'Amount' : [], 'Goal': []}

    for event in events:
        # load the bid index into a BeautifulSoup object, and a named iterator for custom iteration
        soup = BeautifulSoup(requests.get(root.format('/bids/{}'.format(event)), 'r').text, 'html.parser')
        bid_index = soup.find_all('tr')[1:]
        # to add the bid options for each game, we'll need to keep track of the bid we're adding at each iteration
        current_bid = ''

        for tr in bid_index:
            # cases:
            # 1) if there are no td tags, it's a header column for bid options - go to the next row
            # 2) if the first td tag (bid name) has a colspan tag, it's a set of bid options - add them all at once
            # 3) if it doesn't, it's a single row for a big - add it normally
            if len(tr.find_all('td')) == 0:
                pass
            elif tr.find('td').has_attr('colspan'):
                # extract the table of bid choices
                options = [tr for tr in tr.find('table').find_all('tr')[1:] if len(tr.find_all('td')) > 0]

                bids['Bid Name'] += [current_bid for tag in options]
                bids['Bid Choice'] += [clean_text(tag.find('td').get_text()) for tag in options]
                bids['Run'] += [clean_text(tag.find_all('td')[1].get_text()) for tag in options]
                bids['Event'] += [event for tag in options]
                bids['Description'] += [clean_text(tag.find_all('td')[2].get_text()) for tag in options]
                bids['Amount'] += [clean_text(tag.find_all('td')[3].get_text()) for tag in options]
                bids['Goal'] += [clean_text(tag.find_all('td')[4].get_text()) for tag in options]
            else:
                # update the value of current_bid
                current_bid = clean_text(tr.find('td').find('a').get_text())

                # add the row
                bids['Bid Name'].append(current_bid)
                bids['Bid Choice'].append('Bid Total')
                bids['Run'].append(clean_text(tr.find_all('td')[1].get_text()))
                bids['Event'].append(event)
                bids['Description'].append(clean_text(tr.find_all('td')[2].get_text()))
                bids['Amount'].append(clean_text(tr.find_all('td')[3].get_text()))
                bids['Goal'].append(clean_text(tr.find_all('td')[4].get_text()))

    # write the data into a csv file
    write_csv(pd.DataFrame.from_dict(bids), 'bids.csv')


def clean_text(s):
    # if the string is a dollar amount, remove the dollar sign and any commas
    # otherwise, replace commas with spaces
    # in both cases, strip newlines
    if s[0] == '$':
        return s[1:].strip().replace(',', '')
    else:
        return s.strip().replace(',' , ' ')


def write_csv(df, filename):
    # create new file with the specificed filename if it doesn't exist
    f = Path('data\\{}'.format(filename))
    f.touch(exist_ok = True)

    # convert file into a csv
    with open(f, 'w', encoding = 'utf-8') as infile:
        infile.write(df.to_csv(index = False))


if __name__ == '__main__':
    if len(argv) != 2 or argv[1] not in ['donations', 'runs', 'prizes', 'bids']:
        print('Usage: python scraper.py {donations, runs, prizes, bids}')
    elif argv[1] == 'donations':
        scrape_donations()
        print('Donations list scraped into donations.csv!')
    elif argv[1] == 'games':
        scrape_runs()
        print('Runs list scraped into runs.csv!')
    elif argv[1] == 'prizes':
        scrape_prizes()
        print('Prize list scraped into prizes.csv!')
    elif argv[1] == 'bids':
        scrape_bids()
        print('Bids list scraped into bids.csv!')