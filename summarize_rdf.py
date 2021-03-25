import pymysql
import json

from LODCloud import LODCloud

RDF_DATABASE = 'rdf_metadata'
CKAN_TABLE = 'ckan'
SOCRATA_TABLE = 'socrata'
LODCLOUD_TABLE = 'lod-cloud.net'
SUMMARY_TABLE = 'summary'


def select_rdf(db_name, connection):
    sql = 'INSERT INTO `{}`.`{}`(`mimetype`, `cache_url`, `hash`, `description`, `name`, `format`, `url`,' \
          '`datafile_date`, `cache_last_updated`, `package_id`, `created`, `state`, `mimetype_inner`, ' \
          '`last_modified`, `position`, `revision_id`, `url_type`, `id`, `resource_type`, `size`, `license_title`, ' \
          '`maintainer`, `private`, `maintainer_email`, `dataset_id`, `metadata_created`, `metadata_modified`, ' \
          '`author`, `author_email`, `dataset_state`, `version`, `creator_user_id`, `type`, `license_id`, ' \
          '`dataset_name`, `isopen`, `dataset_url`, `notes`, `owner_org`, `title`, `dataset_revision_id`, ' \
          '`org_description`, `org_created`, `org_title`, `org_name`, `org_is_organization`, `org_state`, ' \
          '`org_image_url`, `org_revision_id`, `org_type`, `org_id`, `org_approval_status`, `tags`, `extras`, ' \
          '`data_source`, `db_name`) SELECT `mimetype`, `cache_url`, `hash`, `description`, `name`, `format`, `url`, ' \
          '`datafile_date`, `cache_last_updated`, `package_id`, `created`, `state`, `mimetype_inner`, ' \
          '`last_modified`, `position`, `revision_id`, `url_type`, `id`, `resource_type`, `size`, `license_title`, ' \
          '`maintainer`, `private`, `maintainer_email`, `dataset_id`, `metadata_created`, `metadata_modified`, ' \
          '`author`, `author_email`, `dataset_state`, `version`, `creator_user_id`, `type`, `license_id`, ' \
          '`dataset_name`, `isopen`, `dataset_url`, `notes`, `owner_org`, `title`, `dataset_revision_id`, ' \
          '`org_description`, `org_created`, `org_title`, `org_name`, `org_is_organization`, `org_state`, ' \
          '`org_image_url`, `org_revision_id`, `org_type`, `org_id`, `org_approval_status`, `tags`, `extras`, ' \
          '`data_source`, \'{}\' FROM `metadata`.`{}` WHERE format REGEXP CONCAT_WS(\'|\',\'turtle\',' \
          '\'N-Triples\',\'N-Quads\',\'TriG\',\'rdf\',\'JSON-LD\',\'TriX\',\'^ttl$\', \'[:punct:]ttl\',\'^nt$\', ' \
          '\'[:punct:]nt\',\'^nq$\', \'[:punct:]nq\',\'^owl$\', \'[:punct:]owl\',\'^jsonld$\', \'[:punct:]jsonld\',' \
          '\'^rt$\', \'[:punct:]rt[:punct:]\',\'^rj$\', \'[:punct:]rj\',\'^n3$\', \'[:punct:]n3\') ' \
        .format(RDF_DATABASE, CKAN_TABLE, db_name, db_name)
    # print(sql)
    cur = connection.cursor()
    try:
        cur.execute(sql)
        connection.commit()
        print('insert {} success'.format(db_name))
    except Exception as e:
        print(db_name, e)


def summarize_ckan():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    sql = 'SELECT `name` FROM `portals`.`crawled` WHERE api_type=\'ckan\' AND same_as IS NULL'
    cur = connection.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    total, cnt = len(results), 1
    print('total: ', total)
    for res in results:
        print(cnt, '/', total, res[0])
        select_rdf(res[0], connection)
        cnt += 1
    print('CKAN DONE')
    connection.close()


def summarize_socrata():
    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db='portals',  # 数据库名
                                 )
    sql = 'SELECT `name` FROM `portals`.`crawled` WHERE api_type=\'socrata\''
    cur = connection.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    for res in results:
        db_name = res[0]
        sql = 'INSERT INTO `{}`.`{}`(`name`, `id`, `parent_fxf`, `description`, `attribution`, ' \
              '`attribution_link`, `contact_email`, `type`, `updatedAt`, `createdAt`, `metadata_updated_at`, ' \
              '`data_updated_at`, `page_views_last_week`, `page_views_last_month`, `page_views_total`, ' \
              '`page_views_last_week_log`, `page_views_last_month_log`, `page_views_total_log`, `columns_name`, ' \
              '`columns_field_name`, `columns_datatype`, `columns_description`, `columns_format`, `download_count`, ' \
              '`provenance`, `lens_view_type`, `blob_mime_type`, `hide_from_data_json`, `publication_date`, ' \
              '`categories`, `tags`, `domain_tags`, `domain_metadata`, `domain`, `license`, `permalink`, `link`, ' \
              '`owner_id`, `user_type`, `display_name`, `data_source`, `db_name`)  SELECT `name`, `id`, `parent_fxf`,' \
              '`description`, `attribution`, `attribution_link`, `contact_email`, `type`, `updatedAt`, `createdAt`, ' \
              '`metadata_updated_at`, `data_updated_at`, `page_views_last_week`, `page_views_last_month`, ' \
              '`page_views_total`, `page_views_last_week_log`, `page_views_last_month_log`, `page_views_total_log`, ' \
              '`columns_name`, `columns_field_name`, `columns_datatype`, `columns_description`, `columns_format`, ' \
              '`download_count`, `provenance`, `lens_view_type`, `blob_mime_type`, `hide_from_data_json`, ' \
              '`publication_date`, `categories`, `tags`, `domain_tags`, `domain_metadata`, `domain`, `license`, ' \
              '`permalink`, `link`, `owner_id`, `user_type`, `display_name`, `data_source`, ' \
              '\'{}\' FROM `metadata`.`{}` '.format(RDF_DATABASE, SOCRATA_TABLE, db_name, db_name)
        try:
            cur.execute(sql)
            connection.commit()
            print('insert {} success'.format(db_name))
        except Exception as e:
            print(db_name, e)
    connection.close()


def get_value_from_str(json_str, key):
    json_dic = json.loads(json_str)
    return json_dic.get(key, None)


class SummaryLOD(LODCloud):
    def get_value(self, json_dic):
        value = []
        value.append(self.get_single(json_dic, 'title'))
        value.append(None)
        description_dic = self.get_single(json_dic, 'description')
        value.append(self.get_single(description_dic, 'en'))
        owner_dic = self.get_single(json_dic, 'contact_point')
        value.append(self.get_single(owner_dic, 'name'))
        value.append(self.get_single(json_dic, 'website'))

        full_download = json_dic['full_download']
        if len(full_download) > 0:
            # download = full_download[0]['download_url']
            fd = [x['download_url'] for x in full_download]
            download = ';'.join(fd)
        else:
            download = None
        value.append(download)

        # rest = [full_download[i]['download_url'] for i in range(1, len(full_download))]
        other_download = json_dic['other_download']
        # rest += [x['access_url'] for x in other_download]
        rest = [x['access_url'] for x in other_download]
        other_url = ';'.join(rest)
        if len(other_url) <= 0:
            other_url = None
        value.append(other_url)

        value.append(None)
        value.append(None)
        value.append(self.get_single(json_dic, 'license'))
        value.append(self.get_array(json_dic, 'keywords'))
        value.append(None)
        value.append(None)
        value.append(None)
        value.append(None)
        value.append('lodcloud')
        value.append(self.get_single(json_dic, 'identifier'))
        value.append('lod-cloud.net')
        return value


def summary_lodcloud():
    lod = SummaryLOD()
    file_dic = lod.get_file_dic()

    connection = pymysql.connect(host='localhost',  # host属性
                                 port=3306,
                                 user='root',  # 用户名
                                 password='mysql',  # 此处填登录数据库的密码
                                 db=RDF_DATABASE,  # 数据库名
                                 )
    cur = connection.cursor()
    sql = 'INSERT INTO `{}`.`{}`(`title`, `name`, `description`, `author`, `url`, `download`, `other_url`, `created`, ' \
          '`updated`, `license`, `tags`, `notes`, `dataset_id`, `parent_fxf`, `version`, `source`, `id`, `db_name`) ' \
          'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'\
        .format(RDF_DATABASE, SUMMARY_TABLE)
    values = []
    for id in file_dic.keys():
        data = file_dic[id]
        value = lod.get_value(data)
        values.append(value)
    try:
        # 执行sql语句
        rows = cur.executemany(sql, values)
        # 提交到数据库执行
        connection.commit()
        print('insert {} rows'.format(rows))
        connection.close()
    except Exception as e:
        # 如果发生错误则回滚
        print(e)
        connection.rollback()


def test():
    lod = LODCloud()
    file_dic = lod.get_file_dic()
    data = file_dic['ichoose']
    full_download = data['full_download']
    if len(full_download) > 0:
        download = full_download[0]['download_url']
    else:
        download = None
    print('download', download)

    rest = [full_download[i]['download_url'] for i in range(1, len(full_download))]
    other_download = data['other_download']
    rest += [x['access_url'] for x in other_download]
    same_as = ';'.join(rest)
    if len(same_as) <= 0:
        same_as = None
    print('same as:', same_as)


if __name__ == '__main__':
    summary_lodcloud()
