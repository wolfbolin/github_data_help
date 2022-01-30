
def set_app_pair(conn, app, key, val):
    with conn.cursor() as cursor:
        sql = "INSERT INTO `kvdb` (`app`,`key`,`val`) VALUES (%s,%s,%s)" \
              "ON DUPLICATE KEY UPDATE `val`=VALUES(`val`)"
        cursor.execute(query=sql, args=[app, key, val])
        conn.commit()
        return cursor.rowcount


def get_app_pair(conn, app, key):
    with conn.cursor() as cursor:
        sql = "SELECT `key`, `val` FROM `kvdb` WHERE `app`=%s AND `key`=%s"
        cursor.execute(query=sql, args=[app, key])
        item = cursor.fetchone()
        if item and key == item[0]:
            return item[1]
        else:
            return None


def mysql_replace_sql(table, data):
    sql = "REPLACE INTO `{}`".format(table)
    sql += "(`{}`)".format("`,`".join(data.keys()))
    sql += "VALUES({})".format(("%s," * len(data.items()))[:-1])
    return sql
