import os
from os import listdir
from os.path import isfile, join
import random as random
import pymssql
from datetime import datetime as dt
from datetime import timedelta  
import re
import sys
from random import randrange

class Generator(object):

    server = "dbadmin.iisg.agh.edu.pl"
    user = 'piaskowy'
    password = 'kvwCRv8x2J'
    dbName = 'piaskowy_a'
    connection = 0
    db = 0

    def __init__(self):
        self.db = pymssql.connect(self.server, self.user, self.password, self.dbName)

    def __del__(self):
        self.db.close()

    def genClients(self, how):       
        namesFile = open('data/names.txt', 'r')
        namesSet = namesFile.readlines()
        surnamesFile = open('data/surnames.txt', 'r')
        surnamesSet = surnamesFile.readlines()
        placesFile = open('data/place.txt', 'r')
        places = placesFile.readlines()

        out = []
        sqlOutputs = open('clientsSql.sql', 'a')
        emailSet = [] #email must be unique
        phoneSet = [] #phone must be unique
        loginSet = [] #login must be unique
        acNoSet = [] #account number must be unique
        
        i = 0
        while i < how:
            name = self.getRandElementFromTable(namesSet)
            surname = self.getRandElementFromTable(surnamesSet)
            adress = self.getRandElementFromTable(places)
            adress = adress.split(',')
            nr = adress[2]
            nr = nr.split('/')

            email = name + '.' + surname + '@gmail.com'
            phone = random.randint(528349553, 967942954)
            login = name + surname + str(random.randint(0, 99999))
            pasHash = self.getHash()
            accountNumber = self.getAccountNumber()
            bit = random.randint(0, 1)

            if email in emailSet or phone in emailSet or login in loginSet:
                continue
            
            query = 'INSERT INTO `clients` (name, phone_number, email, password_hash, account_number, city, street, house_number, flat_number, zip, is_company) values ("' \
            + name + ' ' + surname + '", "' \
            + str(phone) + '", "' \
            + email + '", "' \
            + login + '", "' \
            + pasHash + '", "' \
            + accountNumber + '", "' \
            + adress[0] + '", "' \
            + adress[1] + '", "' \
            + nr[0] + '", "' \
            + nr[1] + '", "' \
            + adress[3] + '", ' \
            + str(bit) + ')' 

            if query in out:
                continue
            out.append(query)
            emailSet.append(email)
            phoneSet.append(phone)
            loginSet.append(login)
            i += 1

        for i in out:
            sqlOutputs.write(i + '\n')
        sqlOutputs.close()

    def genConferences(self):
        how = 100
        conferencesNameFile = open('data/conferences_name.txt', 'r')
        conferencesName = conferencesNameFile.readlines()
        conferencesPlaceFile = open('data/conferences_place.txt', 'r')
        conferencesPlace = conferencesPlaceFile.readlines()

        out = []
        fileName = 'clientsSql.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')

        i = 0
        while i < how:
            adress = self.getRandElementFromTable(conferencesPlace)
            adress = adress.split(',')
            name = self.getRandElementFromTable(conferencesName)
            date = self.randomDate(dt.strptime('2016-01-01', '%Y-%m-%d'), dt.strptime('2020-12-12', '%Y-%m-%d'))
            dateArray = str(date).split(' ')
            nextDate = self.randomNextDate(date)
            nextDateArray = str(nextDate).split(' ')
            query = 'exec addNewConference \
            @name = "' + name + '", \
            @date_start = "' + str(dateArray[0]) + '", \
            @date_end = "' + str(nextDateArray[0]) + '", \
            @city = "' + str(adress[0]) + '", \
            @street = "' + str(adress[1]) + '", \
            @local_number = "' + str(adress[2]) + '", \
            @zip = "' + str(adress[3]) + '";'
            if query in out:
                continue
            out.append(query)
            i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close()

    def getConferencesDay(self):
        themesSet = open('data/conferences_theme.txt', 'r').readlines()

        if os.path.exists('confDay.sql'):
            os.remove('confDay.sql')
        sqlOutputs = open('confDay.sql', 'a')
        out = []
        allConf = self.db.cursor(as_dict=True)
        allConf.execute("select * from conferences")

        for conf in allConf:
            i = 0
            dayDate = dt.strptime(conf['date_start'], '%Y-%m-%d')
            date_end = dt.strptime(conf['date_end'], '%Y-%m-%d')
            while dayDate <= date_end:
                query = 'exec addNewConferenceDay \
                @conferences_id = '+ str(conf['id']) + ', \
                @theme = "' + self.getRandElementFromTable(themesSet) + '", \
                @date = "' + str(dayDate)[:10] + '", \
                @time_start = "' + self.getRandTimeStart() + '", \
                @time_end = "' + self.getRandTimeEnd() + '", \
                @max_participants = ' + str(random.randint(16, 300))
                out.append(query)
                dayDate += timedelta(days=1)

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 

    def getWorkshop(self):
        themesSet = open('data/conferences_theme.txt', 'r').readlines()
        fileName = 'workshop.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []
        allConfDay = self.db.cursor(as_dict=True)
        allConfDay.execute("select * from conferences_days")

        for confDay in allConfDay:
            i = 0
            howWorkshop = random.randint(1, 3)
            while i < howWorkshop:
                query = 'exec addNewWorkshop \
                @name = "' + self.getRandElementFromTable(themesSet) + '", \
                @time_start = "' + self.getRandTimeStartWorkshop() + '", \
                @time_end = "' + self.getRandTimeEndWorkshop() + '", \
                @max_participants = ' + str(random.randint(16, confDay['max_participants'])) + ', \
                @price = "' + str(random.randint(1, 200)) + '.' + str(random.randint(0, 100)) + '", \
                @conferences_days_id = '+ str(confDay['id'])
                out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close()   

    def getPriceLevels(self):
        fileName = 'pricelevel.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []
        allConf = self.db.cursor(as_dict=True)
        allConf.execute("select * from conferences")

        for conf in allConf:
            i = 0
            howPriceLevel = random.randint(1, 4)
            dateLvl = dt.strptime(conf['date_start'], '%Y-%m-%d')
            startPrice = random.randint(50, 350)
            while i < howPriceLevel:
                dateEnd = str(dateLvl).split(' ')[0]
                query = 'exec addNewPriceLevel \
                @price = "' + str(startPrice) + '", \
                @date_end = "' + dateEnd + '", \
                @conferences_id = ' + str(conf['id'])
                startPrice *= 0.7
                dateLvl -= timedelta(days=random.randint(1, 10))
                out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 

    def getConferencessDayBooking(self):
        fileName = 'conferencesDayBooking.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []
        allClients = self.db.cursor(as_dict=True)
        allClients.execute("select * from clients")
        clientsList = allClients.fetchall()
        allConfDay = self.db.cursor(as_dict=True)
        allConfDay.execute("select * from conferences_days")
        for confDay in allConfDay:
            confDayDate = dt.strptime(confDay['date'], '%Y-%m-%d')
            maxBookPlace = int(confDay['max_participants']*0.25)-1
            if maxBookPlace < 1: 
                maxBookPlace = 1
            bookingPlace = random.randint(1, maxBookPlace)
            maxRand = random.randint(1, 3)
            i = 0
            while i < maxRand:
                query = 'exec addNewConferenceDaysBooking \
                @conferences_days_id = ' + str(confDay['id']) + ', \
                @booking_date = "' + str(confDayDate - timedelta(days=random.randint(15, 70))).split(' ')[0] + '", \
                @booking_places = ' + str(bookingPlace) + ',\
                @how_students = ' + str(bookingPlace * (1/random.randint(1, 100))) + ', \
                @clients_id = ' + str(self.getRandElementFromTableStandard(clientsList)['id']) + ', \
                @cancel_date = null'
                out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 

    def getWorkshopBooking(self):
        fileName = 'workshopBooking.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []
        allConferencesDayBooking = self.db.cursor(as_dict=True)
        allConferencesDayBooking.execute("select * from conferences_days_bookings")
        allConferencesDayBookingList = allConferencesDayBooking.fetchall()
        allWorkshop = self.db.cursor(as_dict=True)
        allWorkshop.execute("select * from workshops")

        for workshop in allWorkshop:
            maxRand = random.randint(1, 3)
            i = 0
            while i < maxRand:
                bookedInThisTour = 0
                idCDB = self.existRelation(workshop['conferences_days_id'], 'conferences_days_id', allConferencesDayBookingList);
                if idCDB > 0:
                    item = self.getItemById(idCDB, allConferencesDayBookingList)
                    maxBookPlace = int(item['booking_places']*0.25)
                    if maxBookPlace < 1: 
                        maxBookPlace = 1
                    bookingPlace = random.randint(1, maxBookPlace)
                    bookedInThisTour += bookingPlace
                    if bookedInThisTour > item['booking_places']:
                        break
                    query = 'exec addNewWorkshopBooking \
                    @workshop_id = ' + str(workshop['id']) + ', \
                    @booking_place = ' + str(bookingPlace) + ', \
                    @conferences_day_booking_id = ' + str(idCDB)
                    out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 
    
    def getParticipants(self):
        fileName = 'participants.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')

        namesFile = open('data/names.txt', 'r')
        namesSet = namesFile.readlines()
        surnamesFile = open('data/surnames.txt', 'r')
        surnamesSet = surnamesFile.readlines()

        emailSet = []

        out = []
        i = 0
        while i < 3000:
            name = self.getRandElementFromTable(namesSet)
            surname = self.getRandElementFromTable(surnamesSet)
            email = name + '.' + surname + '@gmail.com'
            if email in emailSet:
                continue
            query = 'exec addNewParticipant \
            @firstname = "' + name + '", \
            @surname = "' + surname + '", \
            @email = "' + email
            emailSet.append(email)
            out.append(query)
            i += 1

        for i in out:
            sqlOutputs.write(re.sub("                 ", " ", i) + '\n')
        sqlOutputs.close() 

    def getConferencesDayReservation(self):
        fileName = 'conferencesDayReservation.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []

        tmp = self.db.cursor(as_dict=True)
        tmp.execute("select count(*) as participantsCount from participants")
        for item in tmp:
            participantsCount = item['participantsCount']

        allParticipants = self.db.cursor(as_dict=True)
        allParticipants.execute("select * from participants")
        participantsList = allParticipants.fetchall()
        
        allConferencesDayBooking = self.db.cursor(as_dict=True)
        allConferencesDayBooking.execute("select * from conferences_days_bookings")

        for confDayBooking in allConferencesDayBooking:
            i = 0
            howStudents = 0;
            isStudent = 0
            participantsSet = []
            while i < confDayBooking['booking_places']:
                participant = self.getRandElementFromTableStandard(participantsList)
                if participant['id'] in participantsSet:
                    continue
                if howStudents < confDayBooking['how_students']:
                    isStudent = 1
                    howStudents += 1
                else:
                    isStudent = 0
                query = 'exec addNewConferenceDaysRegistration \
                @conferences_days_booking_id = ' + str(confDayBooking['id']) + ', \
                @participants_id = ' + str(participant['id']) + ',\
                @is_student = ' + str(isStudent)
                participantsSet.append(participant['id'])
                out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 

    def getCompany(self):
        fileName = 'company.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []

        tmp = self.db.cursor(as_dict=True)
        tmp.execute("select count(*) as participantsCount from participants")
        participantsCount = tmp['participantsCount']

        allParticipants = self.db.cursor(as_dict=True)
        allParticipants.execute("select * from participants")
        participantsList = allParticipants.fetchall()
        
        allConferencesDayBooking = self.db.cursor(as_dict=True)
        allConferencesDayBooking.execute("select * from conferences_days_bookings")

        participantsSet = []

        for confDayBooking in allConferencesDayBooking:
            i = 0
            howStudents = 0;
            isStudent = 0
            while i < confDayBooking['booking_places']:
                participant = self.getRandElementFromTableStandard(participantsList)
                if participant['id'] in participantsSet:
                    continue
                if howStudents < confDayBooking['how_students']:
                    isStudent = 1
                    howStudents += 1
                else:
                    isStudent = 0
                query = 'exec addNewConferenceDaysRegistration \
                @conferences_days_booking_id = ' + confDayBooking['id'] + ', \
                @participants_id = ' + participant['id'] + ',\
                @is_student = ' + isStudent

                participantsSet.append(participant['id'])
                out.append(query)
                i += 1

        for i in out:
            sqlOutputs.write(re.sub("                 ", " ", i) + '\n')
        sqlOutputs.close() 

    def getWorkshopReservation(self):
        fileName = 'workshopReservation.sql'
        if os.path.exists(fileName):
            os.remove(fileName)
        sqlOutputs = open(fileName, 'a')
        out = []

        allWorkshopBooking = self.db.cursor(as_dict=True)
        allWorkshopBooking.execute("select * from workshops_booking")

        allConfDayReseg = self.db.cursor(as_dict=True)
        allConfDayReseg.execute("select * from conferences_days_registrations")
        allConfDayResegList = allConfDayReseg.fetchall()

        for workshopBooking in allWorkshopBooking:
            idCDB = self.existRelation(workshopBooking['conferences_days_booking_id'], 'conferences_days_booking_id', allConfDayResegList);
            if idCDB == 0:
                print("aaaaaaaa")
                continue
            item = self.getItemById(idCDB, allConfDayResegList)
            i = 0
            while i < workshopBooking['booking_places']:
                query = 'exec addNewWorkshopRegistration \
                @workshop_booking_id = ' + str(workshopBooking['id']) + ', \
                conferences_days_registrations_id = ' + str(item['id']) 
                out.append(query)
            i += 1

        for i in out:
            sqlOutputs.write(self.cleanItem(i) + '\n')
        sqlOutputs.close() 

    def getPayments(self):
        print("TODO")

##################

    def getItemById(selt, searchId, searchSet):
        for item in searchSet:
            if item['id'] == searchId:
                return item
        return null

    def cleanItem(self, item):
        out = item
        while out.find("  ") > 0:
            out = re.sub("  ", " ", out)
        return out

    def existRelation(self, searchedValue, searchIndex, relationsSet):
        for item in relationsSet:
            if item[searchIndex] == searchedValue:
                return item['id']
        return 0

    def execFile(self, path):
        sqlQueries = open(path, 'r').readlines()
        executor = self.db.cursor(as_dict=True)
        allQuery = 0
        errorQuery = 0
        for query in sqlQueries:
            allQuery += 1
            print(allQuery)
            try:
                executor.execute(query)
            except:
                errorQuery += 1
                print("error: " + str(sys.exc_info()))
        print("Error: " + str(errorQuery))
        print("All: " + str(allQuery))

    def getRandTimeStart(self):
        return str(random.randint(7, 9)) + ':' + str(random.randint(0, 30)) + ":00"

    def getRandTimeEnd(self):
        return str(random.randint(10, 12)) + ':' + str(random.randint(0, 30)) + ":00"

    def getRandTimeStartWorkshop(self):
        return str(random.randint(13, 16)) + ':' + str(random.randint(0, 30)) + ":00"

    def getRandTimeEndWorkshop(self):
        return str(random.randint(17, 20)) + ':' + str(random.randint(0, 30)) + ":00"

    def getRandElementFromTable(self, setArray):
        return setArray[random.randint(0, len(setArray) - 1)].rstrip("\n\r")

    def getRandElementFromTableStandard(self, setArray):
        return setArray[random.randint(0, len(setArray) - 1)]

    def randomDate(self, start, end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)

    def randomNextDate(self, date):
        strNextDate = str(date + timedelta(days=random.randint(0, 5)))
        dateArray = strNextDate.split(' ')
        strOut = dt.strptime(dateArray[0], '%Y-%m-%d')
        return strOut

    def getHash(self):
        chars = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890"
        hashStr = ""
        for i in range(30):
            hashStr += chars[random.randint(0, len(chars) - 1)]
        return hashStr

    def getAccountNumber(self):
        number = ""
        for i in range(24):
            number += str(random.randint(0, 9))
        return number
