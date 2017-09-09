from bs4 import BeautifulSoup
import urllib2
import time

import xmltodict

players_file = 'players.xml'
inning_file = 'inning_all.xml'

def scrape_data():
    '''
    Scrape data from site
    '''
    url_string  = 'http://gd2.mlb.com/components/game/mlb/'
    url = urllib2.urlopen(url_string)

    content = url.read()
    site = BeautifulSoup(content)


    for row in site.findAll('a'):
        if row.attrs.get('href') == 'year_2016/':
            url_year_string = row.attrs.get('href')
            url_year = urllib2.urlopen(url.url + url_year_string)
            time.sleep(3)
            content_year = url_year.read()
            site_year = BeautifulSoup(content_year)
            for month_row in site_year.findAll('a'):
                if month_row.attrs.get('href')[:5] == 'month':
                    url_month_string = month_row.attrs.get('href')
                    url_month = urllib2.urlopen(url.url + url_year_string + url_month_string)
                    time.sleep(3)
                    content_month = url_month.read()
                    site_month = BeautifulSoup(content_month)
                    for day_row in site_month.findAll('a'):
                        if day_row.attrs.get('href')[:3] == 'day':
                            url_day_string = day_row.attrs.get('href')
                            url_day = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string)
                            time.sleep(3)
                            content_day = url_day.read()
                            site_day = BeautifulSoup(content_day)
                            for game_row in site_day.findAll('a'):
                                if game_row.attrs.get('href')[:3] == 'gid':
                                    url_game_string = game_row.attrs.get('href')
                                    url_game = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string)
                                    time.sleep(3)
                                    content_game = url_game.read()
                                    site_game = BeautifulSoup(content_game)
                                    for folder_row in site.findAll('a'):
                                        if folder_row.attrs.get('href') == 'players.xml':
                                            time.sleep(3)
                                            url_players = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + players_file)
                                            with open(url_year_string + url_month_string + url_day_string + url_game_string + players_file, 'wb') as local_file:
                                                local_file.write(url_players.read())
                                        elif folder_row.attrs.get('href') == 'innings/':
                                            url_inning_string = folder_row.attrs.get('href')
                                            url_inning = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_inning_string)
                                            time.sleep(3)
                                            content_inning = url_inning.read()
                                            site_inning = BeautifulSoup(content_inning)
                                            for inning_row in site_inning.findAll('a'):
                                                if inning_row.attrs.get('href') == inning_file:
                                                    time.sleep(3)
                                                    url_inning_file = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_inning_string + inning_file)
                                                    with open(url_year_string + url_month_string + url_day_string + url_game_string + url_inning_string + inning_file, 'wb') as local_file:
                                                        local_file.write(url_inning_file.read())
                                        elif folder_row.attrs.get('href') == 'pitchers/':
                                            url_pitcher_string = folder_row.attrs.get('href')
                                            url_pitcher = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_pitcher_string)
                                            time.sleep(3)
                                            content_pitcher = url_pitcher.read()
                                            site_pitcher = BeautifulSoup(content_pitcher)
                                            for pitcher_row in site_pitcher.findAll('a'):
                                                if pitcher_row.attrs.get('href')[:11] != '/components':
                                                    url_pitcher_id_string = pitcher_row.attrs.get('href')
                                                    time.sleep(3)
                                                    url_pitcher_id = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_pitcher_string + url_pitcher_id_string)
                                                    with open(url_year_string + url_month_string + url_day_string + url_game_string + url_pitcher_string + url_pitcher_id_string, 'wb') as local_file:
                                                        local_file.write(url_pitcher_id.read())
                                        elif folder_row.attrs.get('href') == 'batters/':
                                            url_batter_string = folder_row.attrs.get('href')
                                            url_batter = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_batter_string)
                                            time.sleep(3)
                                            content_batter = url_batter.read()
                                            site_batter = BeautifulSoup(content_batter)
                                            for batter_row in site_batter.findAll('a'):
                                                if batter_row.attrs.get('href')[:11] != '/components':
                                                    url_batter_id_string = batter_row.attrs.get('href')
                                                    time.sleep(3)
                                                    url_batter_id = urllib2.urlopen(url.url + url_year_string + url_month_string + url_day_string + url_game_string + url_batter_string + url_batter_id_string)
                                                    with open(url_year_string + url_month_string + url_day_string + url_game_string + url_batter_string + url_batter_id_string, 'wb') as local_file:
                                                        local_file.write(url_batter_id.read())

# with open('players.xml', 'wb') as local_file:
#     local_file.write(x.read())


def xml_to_json():
    '''
    Turn XML into JSON
    '''
    with open('path/to/file.xml') as fd:
        doc = xmltodict.parse(fd.read())

'''
Writing files
'''
