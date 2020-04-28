import pymysql


def updating_data(list_data):
   try:
    dbconnect = pymysql.connect('zlp18589.vci.att.com', 'techm', 'Techm123@', 'tcoe_support_tools_dev')
    cursor = dbconnect.cursor()
    cursor.execute('select max(id) from t_data_master')
    r = cursor.fetchone()
    '''fetch data for date 
    if not insert else update'''
    d=cursor.execute('select at_date from t_data_master where at_date=DATE_FORMAT(now(),\'%Y-%m-%d\')')
    if d != 0:
        for i in range(len(list_data)):
            sql = "UPDATE t_data_master SET at_total_transaction = {0},at_total_success={1},at_total_errors={2},at_sla_missed={4},at_sla_in={3},at_pass_through={5}," \
                  "orch_500_error={6},arb_500_error={7},orch_400_error={8},arb_400_error={9},predictionistrue={10} where at_model_id={11} and at_date={12}".format(
                str(list_data[i][0]), str(list_data[i][0] - list_data[i][2]), str(list_data[i][2]),
                str(list_data[i][3]),
                str(list_data[i][4]), str(list_data[i][1]),str(list_data[i][5]),str(list_data[i][6]),str(list_data[i][7]),str(list_data[i][8]),str(list_data[i][9]),str(i + 1),str('DATE_FORMAT(NOW(),\'%Y-%m-%d\')'))
            cursor.execute(sql)
            cursor.execute('update metrics_run_count set last_updated_date=NOW()')
    else:
        for i in range(10):
            sql1 = "UPDATE t_data_master SET at_total_transaction = {0},at_total_success={1},at_total_errors={2},at_sla_missed={4},at_sla_in={3},at_pass_through={5}," \
                  "orch_500_error={6},arb_500_error={7},orch_400_error={8},arb_400_error={9},predictionistrue={10}  where at_model_id={11} and at_date={12}".format(
                str(list_data[0][i][0]), str(list_data[0][i][0] - list_data[0][i][2]), str(list_data[0][i][2]),
                str(list_data[0][i][3]),
                str(list_data[0][i][4]), str(list_data[0][i][1]),str(list_data[0][i][5]),str(list_data[0][i][6]),str(list_data[0][i][7]),str(list_data[0][i][8]),str(list_data[0][i][9]),str(i + 1),str('DATE_FORMAT(SUBDATE(NOW(), INTERVAL 1 DAY), \'%Y-%m-%d\')'))
            cursor.execute(sql1)
            sql2 = "insert into t_data_master(id,at_model_id,at_total_transaction,at_total_success,at_total_errors,at_sla_missed,at_sla_in,at_pass_through,orch_500_error" \
                   ",arb_500_error,orch_400_error,arb_400_error,at_date,predictionistrue) values({0},{1},{2},{3},{4},{6},{5},{7},{8},{9},{10},{11},{12},{13})".format(
                str(r[0]+i+1),str(i + 1), str(list_data[1][i][0]),
                str(list_data[1][i][0] - list_data[1][i][2]),
                str(list_data[1][i][2]), str(list_data[1][i][3]),
                str(list_data[1][i][4]), str(list_data[1][i][1]),str(list_data[1][i][5]),str(list_data[1][i][6]),str(list_data[1][i][7]),str(list_data[1][i][8]),str('DATE_FORMAT(NOW(),\'%Y-%m-%d\')'),str(list_data[1][i][9]))
            cursor.execute(sql2)
            cursor.execute('update metrics_run_count set last_updated_date=NOW()')
    dbconnect.commit()
    print('metrics updated successfully in DB')
   except Exception as e:
    print(e)
   finally:
    cursor.close()
    dbconnect.close()

def last_update():
    dbconnect = pymysql.connect('zlp18589.vci.att.com', 'techm', 'Techm123@', 'tcoe_support_tools_dev')
    cursor = dbconnect.cursor()
    d=cursor.execute('select DATE_FORMAT(last_updated_date,\'%Y-%m-%d\') from metrics_run_count where DATE_FORMAT(last_updated_date,\'%Y-%m-%d\')=DATE_FORMAT(now(),\'%Y-%m-%d\')')
    cursor.close()
    dbconnect.close()
    return d

