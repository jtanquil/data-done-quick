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


def read_request_count():
    f = Path('request_counter.txt')
    if not f.is_file():
        f.touch(exist_ok = True)
        
        with open(f, 'w') as infile:
            infile.write('{}\n{}'.format(0, 0))

            return {'pages' : 0, 'donations' : 0}
    else:
        with open(f, 'r') as outfile:
            counts = outfile.read().split('\n')

            return {'pages' : counts[0], 'donations' : counts[1]}


def write_request_count(num_pages = 0, num_donations = 0):
    f = Path('request_counter.txt')
    f.touch(exist_ok = True)

    with open(f, 'w') as infile:
        infile.write('{}\n{}'.format(num_pages, num_donations))
            

def scrape_donations(requests_count):
    # create dicts for donations and donation_bids
    donations = {'id' : [], 'Name' : [], 'Timestamp' : [], 'Amount' : [], 'Comment' : []}
    donation_bids = {'donation_id': [], 'bid_id' : [], 'option_id' : [], 'Amount' : []}
    
    # offset for the next set of donations
    offset_pages = 0
    offset_donations = 0

    # scrape pages while under 30000 requests
    while(offset_pages + offset_donations < 30000):
        # increment the page counter
        offset_pages += 1 
        # load the page into a BeautifulSoup object
        soup = BeautifulSoup(requests.get(root.format('/donations/?page={}'.format(int(requests_count['pages']) + offset_pages)), 'r').text, 'html.parser')
        donation_index = soup.find_all('tr')[1:]

        # update donation columns
        new_ids = [get_id(tr.find_all('td')[2]) for tr in donation_index]

        donations['id'] += new_ids
        donations['Name'] += [clean_text(tr.find_all('td')[0].get_text()) for tr in donation_index]
        donations['Timestamp'] += [clean_text(tr.find_all('td')[1].get_text()) for tr in donation_index]
        donations['Amount'] += [clean_text(tr.find_all('td')[2].get_text()) for tr in donation_index]
        
        # update the donation_bids table and the comments column of donations
        new_comments, new_donation_id, new_amount, new_option_id, new_bid_id, offset_donations = secondary_donations_table(new_ids, offset_donations)
        donations['Comment'] += new_comments
        donation_bids['donation_id'] += new_donation_id
        donation_bids['bid_id'] += new_bid_id
        donation_bids['option_id'] += new_option_id
        donation_bids['Amount'] += new_amount

        print('Pages scraped: {}, Donations scraped: {}'.format(offset_pages, offset_donations), end = '\r')

    print('Total pages scraped: {}, Total donations scraped: {}'.format(offset_pages, offset_donations))     

    # write the data into a csv file - if it's not the top of the table, skip the header
    write_csv(pd.DataFrame.from_dict(donations), 'donations.csv', 'a+', header = False)
    write_csv(pd.DataFrame.from_dict(donation_bids), 'donation_bids.csv', 'a+', header = False)
    # write the new request counts into a text file
    write_request_count(int(requests_count['pages']) + offset_pages, int(requests_count['donations']) + offset_donations)


def secondary_donations_table(donation_ids, num_donations):
    # table that will store the comments scraped from each donation page
    comments, new_donation_id, amount, option_id, bid_id = [], [], [], [], []
    
    # scrape the relevant information from each donation_id on the list
    for donation_id in donation_ids:
        soup = BeautifulSoup(requests.get(root.format('/donation/{}'.format(donation_id)), 'r').text, 'html.parser')
        tables = soup.find_all('table')

        # append the comment, if one exists
        if len(tables) > 0 and clean_text(tables[0].find('th').get_text()) == 'Comment':
            comments.append(clean_text(tables[0].find('td').get_text()))
        else:
            comments.append('N/A')

        # if there are bids, append them
        if len(tables) > 0 and clean_text(tables[-1].find('th').get_text()) == 'Run':
            bids = tables[-1].find_all('tr')[1:]

            new_donation_id += [donation_id for i in range(len(bids))]
            amount += [clean_text(bid.find_all('td')[2].get_text()) for bid in bids]

            # if the bids are options in a bid war, add to the option_id; otherwise add to the bid_id
            for bid in bids:
                if ' -- ' in bid.find_all('td')[1].get_text():
                    option_id.append(get_id(bid.find_all('td')[1]))
                    bid_id.append('N/A')
                else:
                    option_id.append('N/A')
                    bid_id.append(get_id(bid.find_all('td')[1]))

        # increment the donation counter and sleep between donations
        num_donations += 1
        time.sleep(0.1)

    return comments, new_donation_id, amount, option_id, bid_id, num_donations


def get_id(td):
    return td.find('a')['href'].split('/')[-1]


def scrape_runs():
    # load the run index into a BeautifulSoup object
    soup = BeautifulSoup(requests.get(root.format('runs/'), 'r').text, 'html.parser')
    run_index = soup.find_all('tr')
    # create three dicts for the runs, runners and runner_appearances dataframes
    runs, runners, runner_appearances = {}, {}, {}
    
    # extract columns and column names and put them in the appropriate dict
    for i in range(5):
        # remove commas and newlines from text
        col = [clean_text(tr.find_all('td')[i].get_text()) for tr in run_index[1:]]
        colname = clean_text(run_index[0].find_all('th')[i].get_text())

        # for the run name column, also extract the run id from the link in the tag and add it to runs
        if i == 0:
            runs['run_id'] = [tr.find('a').attrs['href'].replace('/tracker/run/', '') for tr in run_index[1:]]

        # add players to runners/runner_appearances, everything else to runs
        if colname == 'Players':
            runners, runner_appearances = secondary_run_tables(runs['run_id'], col)
        else:
            runs[colname] = col
    
    # write data to a csv file
    write_csv(pd.DataFrame.from_dict(runs), 'runs.csv')
    write_csv(pd.DataFrame.from_dict(runners, orient = 'index'), 'runners.csv')
    write_csv(pd.DataFrame.from_dict(runner_appearances), 'runner_appearances.csv')


def secondary_run_tables(run_id_col, players_col):
    # extract individual runners from the run column and add them all to a single list
    parsed_runners = [parse_runners(s) for s in players_col]
    runner_appearances_list = [runner for runner_list in parsed_runners for runner in runner_list]
    # create the runners table from the list of runner appearances
    runners = create_runners(runner_appearances_list)
    # auxiliary runners dict with key/value pairs reversed to create runner_appearances dict
    runners_key = {v:k for (k, v) in runners.items()}

    # create runner_appearances dict
    runner_appearances = {'run_id' : [], 'player_id' : []}

    for run_id, runner_names in zip(run_id_col, parsed_runners):
        runner_appearances['run_id'] += [run_id] * len(runner_names)
        runner_appearances['player_id'] += [runners_key[r] for r in runner_names]

    return runners, runner_appearances


def create_runners(runner_appearances):
    unique_runners = set(runner_appearances)

    return {k:v for k, v in enumerate(unique_runners)}
    

def parse_runners(s):
        return re.split(' and | vs. | or |/| ', s)


def scrape_prizes():
    # load the prize index into a BeautifulSoup object
    soup = BeautifulSoup(requests.get(root.format('prizes/'), 'r').text, 'html.parser')
    prize_index = soup.find_all('tr')
    colnames = []
    columns = []

    # extract colums and column names
    for i in range(7):
        # 1) for the games column, extract the start/end run ids instead
        # 2) skip the image column
        if i == 3:
            # extracting the game column allows us to elegantly extract the start and end run ids
            ids = [tr.find_all('td')[3].find_all('a') for tr in prize_index[1:]]
            
            columns.append([a[0].attrs['href'].replace('/tracker/run/', '') if len(a) > 0 else 'Beginning of marathon' for a in ids])
            colnames.append('start_id')

            columns.append([a[-1].attrs['href'].replace('/tracker/run/', '') if len(a) > 0 else 'End of marathon' for a in ids])
            colnames.append('end_id')
        elif i == 5:
            pass
        else:
            columns.append([clean_text(tr.find_all('td')[i].get_text()) for tr in prize_index[1:]])
            colnames.append(clean_text(prize_index[0].find_all('th')[i].get_text()))
    
    # write data to a csv file
    write_csv(pd.DataFrame.from_dict({k:v for (k, v) in zip(colnames, columns)}), 'prizes.csv')


def scrape_bids():
    # set the list of all events that have a nonempty bid index
    events = ['sgdq2011', 'hrdq'] + [s + y for s in ['agdq', 'sgdq'] for y in [str(i) for i in range(2012, 2019)]]
    # create dicts for bids and bid options
    bids = {'id' : [], 'run' : [], 'event' : [], 'name' : [], 'description' : [], 'goal' : [], 'amount' : []}
    bid_options = {'bid_id' : [], 'id' : [], 'name' : [], 'description' : [], 'amount' : []}

    for event in events:
        # load the bid index into a BeautifulSoup object, and a named iterator for custom iteration
        soup = BeautifulSoup(requests.get(root.format('/bids/{}'.format(event)), 'r').text, 'html.parser')
        bid_index = soup.find_all('tr')[1:]
        # to add the bid options for each game, we'll need to keep track of the bid we're adding at each iteration
        current_bid = ''
        current_bid_id = ''

        for tr in bid_index:
            # cases:
            # 1) if there are no td tags, it's a header column for bid options - go to the next row
            # 2) if the first td tag (bid name) has a colspan tag, it's a set of bid options - add them to bid_options
            # 3) if it doesn't, it's a single row for a big - add it to bids
            if len(tr.find_all('td')) == 0:
                pass
            elif tr.find('td').has_attr('colspan'):
                # extract the table of bid choices
                options = [tr for tr in tr.find('table').find_all('tr')[1:] if len(tr.find_all('td')) > 0]

                # add the rows to bid options
                bid_options['name'] += [clean_text(tag.find('td').get_text()) for tag in options]
                bid_options['bid_id'] += [current_bid_id for tag in options]
                bid_options['id'] += [get_id(tag) for tag in options]
                bid_options['description'] += [clean_text(tag.find_all('td')[2].get_text()) for tag in options]
                bid_options['amount'] += [clean_text(tag.find_all('td')[4].get_text()) for tag in options]
            else:
                # update the value of current_bid and current_bid_id
                current_bid = clean_text(tr.find('td').find('a').get_text())
                current_bid_id = get_id(tr)

                # add the row
                bids['id'].append(current_bid_id)
                bids['name'].append(current_bid)
                bids['run'].append(clean_text(tr.find_all('td')[1].get_text()))
                bids['event'].append(event)
                bids['description'].append(clean_text(tr.find_all('td')[2].get_text()))
                bids['amount'].append(clean_text(tr.find_all('td')[4].get_text()))
                bids['goal'].append(clean_text(tr.find_all('td')[5].get_text()))

    # write the data into a csv file
    write_csv(pd.DataFrame.from_dict(bids), 'bids.csv')
    write_csv(pd.DataFrame.from_dict(bid_options), 'bid_options.csv')


def clean_text(s):
    # if the string is a dollar amount, remove the dollar sign and any commas
    # otherwise, replace commas with spaces
    # in both cases, strip newlines
    s = s.strip()

    if len(s) == 0:
        return s

    if s[0] == '$':
        return s[1:].replace(',', '')
    else:
        return s.replace(',' , ' ')


def write_csv(df, filename, mode = 'w', header = True):
    # create new file with the specificed filename if it doesn't exist
    f = Path('data\\{}'.format(filename))
    f.touch(exist_ok = True)

    # convert file into a csv
    with open(f, mode, encoding = 'utf-8') as infile:
        infile.write(df.to_csv(header = header, index = False))

    print('{} created!'.format(filename))


if __name__ == '__main__':
    if len(argv) != 2 or argv[1] not in ['donations', 'runs', 'prizes', 'bids']:
        print('Usage: python scraper.py {donations, runs, prizes, bids}')
    elif argv[1] == 'donations':
        scrape_donations(read_request_count())
    elif argv[1] == 'runs':
        scrape_runs()
    elif argv[1] == 'prizes':
        scrape_prizes()
    elif argv[1] == 'bids':
        scrape_bids()