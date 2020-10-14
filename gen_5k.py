import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession, Row, Window
from pyspark.conf import SparkConf
from pyspark import StorageLevel
from unidecode import unidecode

spark = SparkSession.builder \
    .appName('PREPROCESS_TR') \
    .config(conf=SparkConf()) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")


def line2triple(l):
    parts = l.split(' ')
    sub = parts[0][1:-1].split('/')[-1]
    pred = parts[1][1:-1].split('/')[-1].split('#')[-1]
    obj = ' '.join(parts[2:-1])
    data = {
        'sub': sub,
        'pred': pred,
        'obj': obj
    }
    return Row(**data)


# First, put data file onto hdfs
# Then, run this code
data_dir = '/project/kgim/data/{}'
company_save_to = 'src.test_company_{}'
person_save_to = 'src.test_person_{}'

triple_sources = [
    ('assetClass', 'src.assetClass'),
    ('currency', 'src.currency'),
    ('industry', 'src.industry'),
    ('organization', 'src.organization'),
    ('person', 'src.person'),
    ('quote', 'src.quote')
]
for filename, tablename in triple_sources:
    triplets = sc.textFile(data_dir.format('reuters/' + filename)) \
        .map(line2triple) \
        .toDF()
    triplets.write.saveAsTable(tablename, mode='overwrite')
    print('save {} to {}'.format(filename, tablename))

clean_str_udf = F.udf(lambda x: x.split('^^')[0].strip('"'), T.StringType())
get_id_udf = F.udf(lambda x: x.split('/')[-1][:-1], T.StringType())
to_uni_udf = F.udf(lambda x: unidecode(x), T.StringType())

len_rank_window = Window.partitionBy('sub').orderBy(F.desc('len'))

# exchange code and ticker
company = spark.read.table('src.origanization') \
    .filter(F.col('pred') == 'hasOrganizationPrimaryQuote') \
    .select('sub', get_id_udf('obj').alias('q')) \
    .dropDuplicates()
ticker = spark.read.table('src,quote') \
    .filter(F.col('pred') == 'hasExchangeTicker') \
    .select(F.col('sub').alias('q'), clean_str_udf('obj').alias('ticker')) \
    .dropDuplicates()
exchange_map = sc.textFile(data_dir.format('tr_sp_exchange_map')) \
    .map(lambda l: l.split('\t')) \
    .toDF(['origin', 'new'])
exchange = spark.read.table('src.quote') \
    .filter(F.col('pred') == 'hasExchangeCode') \
    .select(F.col('sub').alias('q'), clean_str_udf('obj').alias('origin')) \
    .join(exchange_map, 'origin') \
    .select('q', F.col('new').alias('ex_code')) \
    .join(ticker, 'q') \
    .dropDuplicates() \
    .join(company, 'q') \
    .select('sub', F.concat_ws('$', 'ex_code', 'ticker').alias('obj')) \
    .withColumn('pred', F.lit('hasExchange'))
exchange.write.saveAsTable(company_save_to.format('exchange'), mode='overwrite')  # 34772 exchange

# name
clean_name_udf = F.udf(lambda x: x.replace(',', ' ').replace('.', ''), T.StringType())
raw_name = spark.read.table('src.organization') \
    .filter(F.col('pred') == 'organization-name') \
    .select('sub', 'pred', clean_str_udf('obj').alias('origin')) \
    .withColumn('trim', F.trim(F.col('origin'))) \
    .select('sub', 'pred', 'origin', clean_name_udf('obj').alias('obj')) \
    .select('sub', 'pred', 'origin', F.regexp_replace('trim', r'\s+', ' ').alias('obj')) \
    .select('sub', 'pred', 'origin', F.regexp_replace('obj', "(^')|('$)", '').alias('obj')) \
    .select('sub', 'pred', 'origin', to_uni_udf('obj').alias('obj')) \
    .dropDuplicates()
name_count = raw_name.groupBy('sub').count()
raw_name = raw_name.join(name_count, 'sub')
raw_name.persist(StorageLevel.MEMORY_AND_DISK)
conflict = raw_name.filter(F.col('count') > 1) \
    .select('sub', 'pred', 'origin', 'obj')
conflict.write.saveAsTable(company_save_to.format('conflict_name'), mode='overwrite')
name = raw_name.filter(F.col('count') == 1) \
    .select('sub', 'pred', 'origin', 'obj')
name.write.saveAsTable(company_save_to.format('name'), mode='overwrite')  # 5224336 name

# address
clean_address_udf = F.udf(lambda x: x.replace('\\n', ' ').rstrip(), T.StringType())
address = spark.read.table('src.organization') \
    .filter(F.col('pred') == 'HeadquartersAddress') \
    .select('sub', 'pred', clean_str_udf('obj').alias('obj')) \
    .select('sub', 'pred', clean_address_udf('obj').alias('obj')) \
    .select('sub', 'pred', F.regexp_replace('obj', r'\s+', ' ').alias('obj')) \
    .dropDuplicates()
address.persist(StorageLevel.MEMORY_AND_DISK)
addr_count = address.groupBy('sub') \
    .count()
de_dup = address.join(addr_count, 'sub') \
    .withColumn('len', F.length('obj')) \
    .withColumn('rank', F.rank().over(len_rank_window)) \
    .filter(F.col('rank') == 1) \
    .select('sub', 'pred', 'obj')
de_dup.write.saveAsTable(company_save_to.format('address'), mode='overwrite')  # 4904118
address.unpersist()

# country
cfa_udf = F.udf(lambda x: x.split('\\n')[-2], T.StringType())
geo_map = sc.textFile(data_dir.format('tr_sp_country_map')) \
    .map(lambda l: l.split('\t')) \
    .toDF(['g', 'origin', 'new'])
geo_map.persist(StorageLevel.MEMORY_AND_DISK)
country = spark.read.table('src.organization') \
    .filter((F.col('pred') == 'isDomiciledIn') | (F.col('pred') == 'isIncorporatedIn')) \
    .select('sub', 'pred', F.col('obj').alias('g')) \
    .join(geo_map, 'g') \
    .select('sub', 'origin', F.col('new').alias('obj')) \
    .withColumn('pred', F.lit('hasCountry')) \
    .dropDuplicates()
country.write.saveAsTable(company_save_to.format('country'), mode='overwrite')  # 4923466
has_country = spark.read.table('src.organization') \
    .filter(F.col('pred') == 'isDomiciledIn') \
    .select('sub') \
    .dropDuplicates()
has_addr = spark.read.table('src.organization') \
    .filter(F.col('pred') == 'HeadquartersAddress') \
    .join(has_country, 'sub', 'left_anti') \
    .select('sub', clean_str_udf('obj').alias('obj')) \
    .select('sub', cfa_udf('obj').alias('origin')) \
    .join(geo_map, 'origin') \
    .select('sub', 'origin', F.col('new').alias('obj')) \
    .filter(F.col('obj') != '') \
    .withColumn('pred', F.lit('isDomiciledInFromAddress')) \
    .dropDuplicates()
has_addr.write.saveAsTable(company_save_to.format('country'), mode='append')  # 5338

# industry
industry_preds = [
    'hasPrimaryIndustryGroup',
    'hasPrimaryBusinessSector',
    'hasPrimaryEconomicSector'
]
industry_map = sc.textFile(data_dir.format('tr_industry_map')) \
    .map(lambda l: l.split('\t')) \
    .toDF(['origin', 'new'])
industry = spark.read.table('src.industry') \
    .filter(F.col('pred') == 'label') \
    .select('sub', clean_str_udf('obj').alias('obj')) \
    .select(F.col('sub').alias('ind'), F.col('obj').alias('origin')) \
    .join(industry_map, 'origin')
raw_comp_industry = spark.read.table('src.organization') \
    .filter((F.col('pred') == 'hasPrimaryIndustryGroup')
            | (F.col('pred') == 'hasPrimaryBusinessSector')
            | (F.col('pred') == 'hasPrimaryEconomicSector')) \
    .select('sub', get_id_udf('obj').alias('ind')) \
    .join(industry, 'ind') \
    .select('sub', 'origin', F.col('new').alias('obj')) \
    .dropDuplicates()
raw_comp_industry.persist(StorageLevel.MEMORY_AND_DISK)
ind_count = raw_comp_industry.select('sub', 'obj') \
    .dropDuplicates() \
    .groupBy('sub').count()
comp_industry = raw_comp_industry.join(ind_count, 'sub')
comp_industry.persist(StorageLevel.MEMORY_AND_DISK)
no_dup = comp_industry.filter(F.col('count') == 1)
de_dup = comp_industry.filter((F.col('count') > 1) & (F.col('obj') == 'Real Estate')) \
    .unionByName(no_dup) \
    .select('sub', 'obj') \
    .dropDuplicates() \
    .withColumn('pred', F.lit('hasIndustry'))
de_dup.write.saveAsTable(company_save_to.format('industry'), mode='overwrite')  # 896379

# URL
get_domain_udf = F.udf(lambda u: u[1:-1].split('//')[-1].split('/')[0], T.StringType())
url = spark.read.table('src.organization') \
    .filter(F.col('pred') == 'hasURL') \
    .select('sub', 'pred', get_domain_udf('obj').alias('obj')) \
    .dropDuplicates()
url.persist(StorageLevel.MEMORY_AND_DISK)
url_count = url.groupBy('sub') \
    .count()
de_dup = url.join(url_count, 'sub') \
    .withColumn('len', F.length('obj')) \
    .withColumn('rank', F.rank().over(len_rank_window)) \
    .filter(F.col('rank') == 1) \
    .select('sub', 'pred', 'obj')
de_dup.write.saveAsTable(company_save_to.format('url'), mode='overwrite')  # 854966

# family-name of Person
fn = spark.read.table('src.person') \
    .filter(F.col('pred') == 'family-name') \
    .select('sub', 'pred', clean_str_udf('obj').alias('obj')) \
    .dropDuplicates() \
    .filter(((F.col('sub') != '1-34413176823') | (F.col('obj') != 'OBrien'))
            & ((F.col('sub') != '1-34423120002') | (F.col('obj') != 'Monin de Flaugergues'))
            & ((F.col('sub') != '1-34423753018') | (F.col('obj') != 'Chan M.P.')))
fn.write.saveAsTable(person_save_to.format('familyname'), mode='overwrite')  # 4667784

# given-name of Person
gn = spark.read.table('src.person') \
    .filter(F.col('pred') == 'given-name') \
    .select('sub', 'pred', clean_str_udf('obj').alias('obj')) \
    .dropDuplicates() \
    .filter(((F.col('sub') != '1-34413846240') | (F.col('obj') != 'Zhi Wei'))
            & ((F.col('sub') != '1-34414106636') | (F.col('obj') != 'Ginny'))
            & ((F.col('sub') != '1-34414227643') | (F.col('obj') != 'Herman'))
            & ((F.col('sub') != '1-34424198806') | (F.col('obj') != 'Eric')))
gn.write.saveAsTable(person_save_to.format('givenname'), mode='overwrite')  # 4667785

# employee
fn = spark.read.table(person_save_to.format('familyname')) \
    .select(F.col('sub').alias('p'), F.col('obj').alias('fn'))
gn = spark.read.table(person_save_to.format('givenname')) \
    .select(F.col('sub').alias('p'), F.col('obj').alias('gn'))
holders = spark.read.table('src.person') \
    .filter((F.col('pred') == 'hasHolder')) \
    .select(F.col('sub').alias('h'), get_id_udf('obj').alias('p')) \
    .dropDuplicates()
employee = spark.read.table('src.person') \
    .filter((F.col('pred') == 'isTenureIn') | (F.col('pred') == 'isPositionIn')) \
    .select(F.col('sub').alias('h'), get_id_udf('obj').alias('sub')) \
    .join(holders, 'h') \
    .select('sub', 'p') \
    .dropDuplicates() \
    .join(fn, 'p') \
    .join(gn, 'p') \
    .select('sub', F.concat_ws(' ', 'fn', 'gn').alias('obj')) \
    .select('sub', F.regexp_replace('obj', r'\s+', ' ').alias('obj')) \
    .dropDuplicates() \
    .withColumn('pred', F.lit('hasEmployee'))
employee.write.saveAsTable(company_save_to.format('employee'), mode='overwrite')  # 2480390
