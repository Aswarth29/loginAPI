import sys
from bs4 import BeautifulSoup
import pandas as pd
import os
import numpy as np
import configparser
import snowflake.connector
from codecs import open






# def checkTag1(tagName):
#     calcRes = Bs_data.find(tagName)
#     if (calcRes is None):
#         isTagExist = False
#     else:
#         isTagExist = True
#     return isTagExist, calcRes


def ProCollValues(proTag,calcRes):
    vWFormula = ""
    vWST = ""

    isTagExist = ""
    vDictProj = {}
    vDictJoin = {}
    for tempPT in proTag:

        vFormula = ""
        vST = ""
        projList = []
        joinList = []
        vProId = tempPT.get('id')
        vIDStr = ""
        pro1Table = tempPT.input.get('node')

        # print(vProId, " ", pro1Table)#Join_1   V_SALARIES
        if 'Join' in vProId:
            vJoinId = tempPT.get('id')
            # print(vJoinId)#Join_1
            vJoinType = tempPT.get('joinType')
            # print(vJoinType)inner
            joinTag = tempPT.find_all('input')
            # print(joinTag )
            for TempTag in joinTag:
                vTemp = TempTag.get('node')
                if "#" in vTemp:
                    vTemp = vTemp.replace('#', '')
                joinList.append(vTemp)
            vDictJoin[vJoinId] = joinList
            continue

        if "Join" in vProId:
            continue
        if "#" in pro1Table:
            pro1Table = pro1Table.replace('#', '')
        calcResColl = tempPT.find("calculatedViewAttributes")

        if (calcResColl is None):
            isTagExist = False
        else:
            # print("calcResColl")
            proColl = tempPT.calculatedViewAttributes.find_all("calculatedViewAttributes")
            for cvColl in proColl:
                vID = cvColl.get('id')

                if vFormula == "":
                    vFormula = vProId + "." + vID
                    vIDStr = vID
                else:
                    vFormula += "  ,  " + vProId + "." + vID
                    vIDStr += " , " + vID
                projList.append(vID)
        proColl = tempPT.input.find_all("mapping")
        for cvColl in proColl:
            vtarget = cvColl.get('target')
            vSource = cvColl.get('source')
            if vtarget != vSource:
                if vST == "":
                    vST = vProId + "." + vtarget
                    vIDStr = vtarget
                else:
                    vST += "  ,  " + vProId + "." + vtarget
                    vIDStr += " , " + vtarget
            projList.append(vtarget)
        vProData = "Projection: " + vProId + "\n" + "ProjectionNode :" + pro1Table
        if vFormula != "":
            if vWFormula == "":
                vWFormula = "\n" + "Formula :" + vFormula
            else:
                vWFormula += ", " + vFormula + "\n"
        if vST != "":
            if vWST == "":
                vWST = "vST: " + vST
            else:
                vWST += ", " + vST + "\n"
        vDictProj[vProId] = np.array(projList)

        # NPData+= vProData+vWFormula+vWST+vIDStr+"\n\n"
    # df=pd.DataFrame.from_dict(vDictProj)
    dfProj = pd.DataFrame.from_dict(vDictProj, orient='index')
    dfProj = dfProj.transpose()
    proTag = calcRes.logicalModel.attributes.find_all("attribute")
    return dfProj, proTag, pro1Table, vDictJoin


def funcCollList(proTag,calcRes):
    vOrderPrev = 0
    for tempPT in proTag:
        vOrder = int(tempPT.get('order'))
        if vOrderPrev == 0:
            vOrderPrev = vOrder
        elif vOrderPrev <= vOrder:
            vOrderPrev = vOrder

    proTag = calcRes.logicalModel.baseMeasures.find_all("measure")
    for tempPT in proTag:
        vOrder = int(tempPT.get('order'))
        if vOrderPrev == 0:
            vOrderPrev = vOrder
        elif vOrderPrev <= vOrder:
            vOrderPrev = vOrder

    vCollList = []
    for i in range(vOrderPrev):
        tempVal01 = "## NoCollName-Order num- " + str(i + 1) + "##"
        vCollList.append(tempVal01)
    proTag = calcRes.logicalModel.attributes.find_all("attribute")
    vOrderPrev = 0
    for tempPT in proTag:
        vOrder = (int(tempPT.get('order')) - 1)
        vId = tempPT.get('id')
        vCollList[vOrder] = vId

    proTag = calcRes.logicalModel.baseMeasures.find_all("measure")
    for tempPT in proTag:
        vOrder = (int(tempPT.get('order')) - 1)
        vId = tempPT.get('id')
        vCollList[vOrder] = vId
    return vCollList


# select columns
def join1(proTag,calcRes):
    proTag = calcRes.logicalModel.find_all("attributes")
    test1 = []  # getting id from attribute

    for i in proTag:
        j = i.find_all('attribute')
        for k in j:
            l = k.get('id')
            # print(l)
            test1.append(l)
    # print('test1',test1)
    proTag = calcRes.logicalModel.baseMeasures.find_all("measure")
    test2 = []  # getting id from source
    for i in proTag:
        j = i.get('id')
        test2.append(j)
    # print('test2',test2)
    proTag = calcRes.calculationViews.find_all("calculationView")
    # joinList=[]
    # vTemp=[]
    joining = []  # select columns
    groupby = []  # groupby condition
    proTag1 = calcRes.logicalModel.baseMeasures.find_all("measure")
    mylist = []  # getting id
    mylist1 = []  # contains aggregation type
    for tempPT1 in proTag1:
        vId = tempPT1.get('id')
        mylist.append(vId)
        tId = tempPT1.get('aggregationType')
        mylist1.append(tId)
    system_data_dict = {sys: inst for sys, inst in zip(mylist, mylist1)}
    # print('system_data_dict',system_data_dict)
    Getkeys = system_data_dict.keys()
    keylist = list(Getkeys)
    # print('keylist',keylist)
    # test3-combining two list
    test3 = test1 + test2
    # print('mylist',mylist)
    # print('mylist1',mylist1)
    # print('test3',test3)

    test4 = []  # source value
    final1 = []  # table name

    for tempPT in proTag:
        joinTag = tempPT.find_all('input')
        for TempTag in joinTag:
            vTemp = TempTag.get('node')
            # final1.append(vTemp)
            for TempTag1 in TempTag.find_all('mapping'):
                vtesttemp = TempTag1.get('source')
                test4.append(vtesttemp)
                final1.append(vTemp)
    # print('test4',test4)
    # print('final1',final1)

    system_data_dict1 = {sys: inst for sys, inst in zip(test4, final1)}
    Getkeys1 = system_data_dict1.keys()
    Keylist1 = list(Getkeys1)  # contains all id

    keylist = list(Getkeys)  # contains measurable id

    for id in Keylist1:
        if id in keylist:
            value1 = (system_data_dict[id])

            value2 = (system_data_dict1[id])

            combinevalue = value1 + "(" + str(value2) + "." + id + ")"
            joining.append(combinevalue)
        else:
            value2 = (system_data_dict1[id])
            combinevalue = str(value2) + "." + id
            joining.append(combinevalue)
            groupby.append(combinevalue)
    return joining, groupby, system_data_dict


def funcUpdate(vTN, joining, vCollList, proTag, groupby, system_data_dict,calcRes):
    proTag = calcRes.calculationViews.find_all("calculationView")
    calTag = calcRes.logicalModel.get('id')

    if calTag == "Join_1":
        joinList = []
        vTemp = []
        # where condition
        joinattribute = []
        for tempPT in proTag:
            joinTag = tempPT.find_all('input')
            for TempTag in joinTag:
                vTemp = TempTag.get('node')
                if "#" in vTemp:
                    vTemp = vTemp.replace('#', '')
                joinList.append(vTemp)
        for temPT in proTag:
            joinTag = tempPT.find_all('joinAttribute')
            for TempTag in joinTag:
                vTemp = TempTag.get('name')
                joinattribute.append(vTemp)
        a = joinList[0]
        b = joinList[1]
        NPData = "CREATE OR REPLACE VIEW " + vTN + " (" + str((','.join(vCollList))) + "\n) AS SELECT\n" + str(
            (','.join(joining))) + " FROM " + str((','.join(joinList))) + " WHERE " + a + "." + str(
            (','.join(joinattribute))) + "=" + b + "." + str((','.join(joinattribute))) + " GROUP BY " + str(
            (','.join(groupby))) + ";"

    else:
        Getkeys = system_data_dict.keys()
        keylist = list(Getkeys)
        joiningcolumns = []
        joininggroupby = []
        for i in vCollList:
            if i in keylist:
                x = system_data_dict[i]
                a = x + "(" + i + ")"
                joiningcolumns.append(a)
            else:
                a = i
                joiningcolumns.append(a)
                joininggroupby.append(a)
        proTag = calcRes.calculationViews.find_all("calculationView")
        tablename = []
        for tempPT in proTag:
            joinTag = tempPT.find_all('input')
            for TempTag in joinTag:
                vTemp = TempTag.get('node')
                tablename.append(vTemp)
        NPData = "CREATE OR REPLACE VIEW " + vTN + " (" + str((','.join(vCollList))) + "\n) AS SELECT\n" + str(
            (','.join(joiningcolumns))) + " FROM " + str((','.join(tablename))) + " GROUP BY " + str(
            (','.join(joininggroupby))) + ";"

    return NPData



def Calcmain(XmlFIle):

    #print(XmlFIle)
    with open(XmlFIle, 'r') as f:
                data = f.read()

    def checkTag(tagName):
        # print("data_value:", data)
        Bs_data = BeautifulSoup(data, "xml")
        # Bs_data = BeautifulSoup
        # print(Bs_data)
        value = Bs_data.find_all(tagName)
        #print(value)
        calcRes = Bs_data.find(tagName)
        if (calcRes is None):
            isTagExist = False
        else:
            isTagExist = True
        return isTagExist, calcRes
    isTagExist, calcRes = checkTag("Calculation:scenario")
    if isTagExist == False:
        sys.exit()
    proTag = calcRes.calculationViews.find_all("calculationView")
    vTN = calcRes.descriptions.get('defaultDescription')
    dfProj, proTag, pro1Table, vDictJoin = ProCollValues(proTag,calcRes)
    vCollList = funcCollList(proTag,calcRes)
    joining, groupby, system_data_dict = join1(proTag,calcRes)
    #print(vTN,joining,vCollList,proTag,groupby,system_data_dict)
    query = funcUpdate(vTN, joining, vCollList, proTag, groupby, system_data_dict,calcRes)
    return query

