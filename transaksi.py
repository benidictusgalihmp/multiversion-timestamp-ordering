READFLAG = 0
WRITEFLAG = 1

class Schedule : 
    def __init__(self, activity, data):
        self.activity = activity            # activity = [W1X, W2X, R3X, ...]
        self.data = data                    # data = [X, Y, Z]
        read = {
            "timestamp" : 0,
            "data" : "-"
        }
        write = {
            "timestamp" : 0,
            "data" : "-"
        }
        version = [read, write]
        self.version = version              # version = [read, write]

    '''
    * GET AND SET METHOD
    '''
    def getActivity(self):
        return self.activity

    def getReadTimestamp(self):
        return self.version[READFLAG]["timestamp"]

    def setReadTimestamp(self, newTS):
        self.version[READFLAG]["timestamp"] = newTS   

    def getWriteTimestamp(self):
        return self.version[WRITEFLAG]["timestamp"]
    
    def setWriteTimestamp(self, newTS):
        self.version[WRITEFLAG]["timestamp"] = newTS

    def setWrite(self, idData, col, newData):
        self.data[idData][col] = newData

    def getData(self):
        return self.data

    def setData(self, dataName, newData):
        self.data[dataName] = newData

    '''
    * CUSTOM METHOD
    '''
    def sparseCommand(self):
        # delimiter 
        arrSparse = []
        for i in range(len(self.activity)) :
            arrSparse.append(list(self.activity[i]))
        return arrSparse
        # output : [[W,1,X],[W,2,X]]    

class Transaksi(Schedule):
    def __init__(self, id, activity, data):
        super().__init__(activity, data)
        self.id = id
        self.rollback = False
        self.arrCommit = []

    '''
    * GET AND SET METHOD
    '''
    def getID(self):
        return self.id

    def getRollback(self):
        return self.rollback

    def setRollback(self, flag):
        self.rollback = flag
    
    '''
    input   : dictionary
    '''    
    def read(self, idData, data, rts):        
        if(rts < self.id):
            self.setReadTimestamp(self.id)
        
        print("READ FILE TRANSACTION "+ str(self.id) + "...")
        
        for dict in data[idData]:
            print(data[idData][dict])
        print()
        
    '''
    input   : data update, dictinary, wtimestamp, rtimestamp
    output  : new wtimestamp and rtimestamp
    '''
    def write(self, idData, col, wts, rts):
        if(self.id < rts):
            self.rollback = True
            pass    # rollback             
        
        # write in local transaction
        varInput = "MAHASISWA-NIM"
        var = [idData, col, varInput]
        self.arrCommit.append(var)        

        if(self.id > wts):
            self.setReadTimestamp(self.id)
            self.setWriteTimestamp(self.id)

    def commit(self):
        if(len(self.arrCommit) == 0 or self.getRollback):
            print("Transaction need to rollback")
            pass
        else:
            for act in self.arrCommit:
                idData = act[0]
                col = act[1]
                varInput = act[2]
                self.setWrite(idData, col, varInput) # write data            
            self.arrCommit = []

############################################
# main program
############################################
xVar = {
    'X' : {
        "nama" : "studentX",
        "nim" : "13519999"
    }, 'Y' : {
        "nama" : "studentY",
        "nim" : "13519995"
    }, 'Z' : {
        "nama" : "studentZ",
        "nim" : "13519991"
    }
}

activity = ['W1X', 'W2Y', 'R3Z', 'R2Y', 'C1', 'C2', 'C3']

trans1 = Transaksi(1, activity, xVar)
trans2 = Transaksi(2, activity, xVar)
trans3 = Transaksi(3, activity, xVar)
arrTrans = [trans1, trans2, trans3]

schedule = Schedule(activity, xVar)
arrExecute = schedule.sparseCommand()
# output : [[W,1,X], [W,2,X], [R,3,Z]] 

print(activity)
for c in arrExecute:
    idTransc = int(c[1]) - 1
    transID = arrTrans[idTransc]

    if(transID.getRollback()):
        # rewrite the activity
        transID.setRollback(False)
        # rollback transaction with ID = C[1]
        break
    else:        
        wts = transID.getWriteTimestamp()
        rts = transID.getReadTimestamp()
        data = transID.getData()
        print('TS = '+ str(idTransc) +'; WTS = ' + str(wts) + '; RTS = ' + str(rts) + '\n')

        if(c[0] == 'R'):
            transID.read(c[2], data, rts)
            print('> READ FILE ON T' + str(c[1]))
        elif(c[0] == 'W'):
            transID.write(c[2], data, wts, rts)
            print('> WRITE FILE ON T' + str(c[1]))
        elif(c[0] == 'C'):
            transID.commit()
            print('> COMMIT FILE ON T' + str(c[1]))
        else:
            print('unknown command!')