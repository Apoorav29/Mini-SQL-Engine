import sys
import re


class QueryHandler:
	join_cndns = []
	dictionary = {}
	def __init__(self, query_str):
		self.query = query_str


	def cartesian_prd(self, table1, table2):
		prod_table = {}
		prod_table['table'] = []
		temp1 = []
		prod_table['info'] = []
		temp2 = []
		for field in table1['info']:
			len_field = len(field.split('.')) 
			if len_field == 1:
				temp1.append(table1['name'] + '.' + field)
			elif len_field != 1:
				temp1.append(field)

		for field in table2['info']:
			len_field = len(field.split('.')) 
			if  len_field == 1:
				temp2.append(table2['name'] + '.' + field)
			elif len_field != 1:
				temp2.append(field)


		prod_table['info'] = prod_table['info']
		prod_table['info'] += temp1 + temp2

		for row1 in table1['table']:
			for row2 in table2['table']:
				app_el = row1+row2
				prod_table['table'].append(app_el)

		return prod_table

	def display_res(self, table):
		table_str = ','.join(table['info']) 
		print(table_str)
		for row in table['table']:
			tab_str = ','.join([str(x) for x in row]) 
			print(tab_str)

	def select(self, tables, condition_str):
		
		result_table = {}
		len_tables = len(tables)
		if len_tables == 1:
			joined_table = self.cartesian_prd(self.dictionary[tables[0]], {'info': [], 'table': [[]]})

		else:
			joined_table = self.cartesian_prd(self.dictionary[tables[0]], self.dictionary[tables[1]])

		if len_tables > 2:
			for i in range(len(tables) - 2):
				joined_table = self.cartesian_prd(joined_table, self.dictionary[tables[i + 2]])

		result_table['info'] = []
		result_table['table'] = []
		count=0
		for x in joined_table['info']:
			count+=1
			result_table['info'].append(x)

		if condition_str == "":
			for row in joined_table['table']:
				app_row = row
				result_table['table'].append(app_row)

		else:
			condition_str = re.sub('(?<=[\w ])(=)(?=[\w ])', '==', condition_str)

			conditions = condition_str.replace(" and ", ",").replace(" or ", ",")
			conditions = conditions.replace('(', '').replace(')', '').split(',')

			for els,condition in enumerate(conditions):
				if bool(re.match('.*==.*[a-zA-Z]+.*', condition.strip())):
					temp1 = condition.strip().split('==')[0].strip()
					# print(els)
					temp2 = condition.strip()
					temp2 = temp2.split('==')[1]
					temp2 = temp2.strip()
					join_cndn = (temp1, temp2)
					self.join_cndns.append(join_cndn)

			for field in joined_table['info']:
					condition_str = condition_str.replace(field, 'row[' + str(joined_table['info'].index(field)) + ']')
			emp_arr = []
			# result_table['table'] = []
			emp_arr.append(result_table['table'])
			for row in joined_table['table']:
				if eval(condition_str):
					result_table['table'].append(row)

		return result_table

	def project(self, table, fields, dist_flag, aggr_flag):

		result_table = {}
		result_table['table'] = []
		dict_labels = {}
		result_table['info'] = []

		if aggr_flag is not None:
			app_str = aggr_flag + "(" + fields[0] + ")"
			result_table['info'].append(app_str)
			temp = []
			field_index = table['info'].index(fields[0])
			count=0
			for row in table['table']:
				count+=1
				temp.append(row[field_index])

			aggr_dict = {
				'sum':sum(temp),
				'min':min(temp),
				'avg':(sum(temp) * 1.0)/len(temp),
				'max':max(temp)
			}

			count+=1
			result_table['table'].append([aggr_dict[aggr_flag]])

		else:
			if fields[0] == '*':
				count=0
				temp = []
				for els,x in enumerate(table['info']):
					count+=1
					temp.append(x)

				fields[:] = temp[:]

				# for els,field_pair in enumerate(self.join_cndns):
				# 	fields[:] = temp[:]

			field_indices = []
			result_table['info'] += fields

			for field in fields:
				field_indices.append(table['info'].index(field))
				# print(count)

			for row in table['table']:
				result_row = []
				for i in field_indices:
					temp_el = row[i]
					result_row.append(temp_el)
				result_table['table'].append(result_row)

			if dist_flag == True:
				temp = sorted(result_table['table'])
				emp_arr = []
				result_table['table'][:] = emp_arr
				for i in range(len(temp)):
					len_temp = len(temp)
					if i == 0 or temp[i] != temp[i-1]:
						result_table['table'].append(temp[i])	

		return result_table

	def semi_err(self, query):
		if query[len(query) - 1] != ';':
			print("Semicolon missing")
			return 1
		return 0

	def format_error(self, query):
		if bool(re.match('^select.*from.*', query)) is False:
			print("Invalid query")
			return 1
		else:
			return 0

	def aggr_error(self, aggr_flag, leng):
		if aggr_flag is not None and leng > 1:
			print("Too many arguments")
			return 1
		return 0

	def tab_err(self, table):
		if table not in self.dictionary:
				print("Invalid table - " + table)
				return 1
		return 0

	def check_field_validity(self, fields, tables):
		field_len = len(fields)
		for field in fields:
			field_flag = 0
			for table in tables:
				if field.split('.')[-1] in self.dictionary[table]['info']:
					len_field = len(field.split('.'))
					if len(field.split('.')) == 2:
						if field.split('.')[0] == table:
							field_flag = 1
						else:
							continue
					else:
						field_flag += 1
			if field_flag!=1:
				print("invalid field")
				return 0

		return 1

	def parse(self):
		dist_flag = False
		aggr_flag = None
		star_flag = False

		if self.semi_err(self.query):
			return

		self.query = self.query.strip(';')
		
		if self.format_error(self.query):
			return

		fields = self.query.split('from')[0]
		fields = fields.replace('select', '').strip()

		if bool(re.match('^distinct.*', fields)):
			fields = fields.replace('distinct', '').strip('()')
			dist_flag = True
			fields = fields.strip()

		if bool(re.match('^(sum|max|min|avg)\(.*\)', fields)):
			aggr_flag = fields.split('(')[0].strip()
			fields = fields.replace(aggr_flag, '').strip()
			fields = fields.strip('()')

		fields = fields.split(',')
		len_fields = len(fields)
		for i in range(len_fields):
			fields[i] = fields[i].strip()

		len_fields = len(fields)
		if len_fields == 1: 
			if fields[0] == '*':
				star_flag = True

		if self.aggr_error(aggr_flag, len(fields)):
			return

		tables = self.query.split('from')[1]
		tables = tables.split('where')[0].strip().split(',')
		tab_len = len(tables)
		for i in range(tab_len):
			tables[i] = tables[i].strip()

		for table in tables:
			if self.tab_err(table):
				return
			else:
				continue

		if bool(re.match('^select.*from.*where.*', self.query)):
			if star_flag is False:
				if self.check_field_validity(fields, tables) == 0:
					return

			condition_str = self.query.split('where')[1].strip()
			temp = condition_str.replace(' and ', ' ')
			emp_arr = []
			emp_arr.append(temp)
			temp = temp.replace(' or ', ' ')

			cond_cols = re.findall(r"[a-zA-Z][\w\.]*", temp)
			cond_cols = list(set(cond_cols))

			ret_flag = self.check_field_validity(cond_cols, tables) 
			if  ret_flag == 0:
				return

			if star_flag is False:
				len_fields = len(fields)
				for i in range(len_fields):
					if len(fields[i].split('.')) == 1:
						len_tab = len(tables)
						for table in tables:
							appended_name = table + '.'
							if fields[i] not in self.dictionary[table]['info']:
								continue
							# else:
							fields[i] = appended_name + fields[i]
							break

			for field in cond_cols:
				len_split = len(field.split('.')) 
				if len_split == 1:
					for table in tables:
						if field not in self.dictionary[table]['info']:
							continue
						else:
							temp1 = table + '.' 
							temp1 += field
							temp2 = ' '
							temp2 += condition_str
							condition_str = re.sub('(?<=[^a-zA-Z0-9])(' + field + ')(?=[\(\)= ])', temp1, temp2)
							condition_str = condition_str.strip(' ')

			self.display_res(self.project(self.select(tables, condition_str), fields, dist_flag, aggr_flag))

		elif bool(re.match('^select.*from.*', self.query)):
			if star_flag is False:
				if self.check_field_validity(fields, tables) == 0:
					return
			if star_flag is False:
				for i in range(len(fields)):
					len_var = len(fields[i].split('.'))
					if len(fields[i].split('.')) == 1:
						for table in tables:
							appended_name = table + '.'
							if fields[i] not in self.dictionary[table]['info']:
								continue
							else:
								fields[i] = appended_name + fields[i]
								break
			self.display_res(self.project(self.select(tables, ""), fields, dist_flag, aggr_flag))

		else:
			if star_flag is not True:
				for field in fields:
					if field in self.dictionary[tables[0]]['info']:
						continue
					else:
						print("Invalid field - " + field)
						return

			self.display_res(self.project(self.dictionary[tables[0]], fields, dist_flag, aggr_flag)) 

	def read_meta_data(self):
		f = open('./metadata.txt', 'r+')
		# with open('./metadata.txt', 'r') as f:
		line = f.readline().strip()

		while line:
			if line == "<begin_table>":
				table_name = f.readline().strip()
				self.dictionary[table_name] = {}
				self.dictionary[table_name]['info'] = []
				attr = f.readline().strip()
				while attr != "<end_table>":
					self.dictionary[table_name]['info'].append(attr)
					attr = f.readline().strip()

			line = f.readline().strip()

		for table_name in self.dictionary:
			self.dictionary[table_name]['table'] = []
			self.dictionary[table_name]['name'] = table_name
			with open ('./' + table_name + '.csv', 'r') as f:
				for line in f:
					self.dictionary[table_name]['table'].append([int(field.strip('"')) for field in line.strip().split(',')])

def main():
	query = sys.argv[1]
	if query.strip() == "":
		print("invlaid query")
		return
	query = query.strip('"').strip().replace("SELECT ", "select ")
	query = query.replace("DISTINCT ", "distinct ").replace("FROM ", "from ")
	query = query.replace("WHERE ", "where ").replace("AND ", "and ").replace("OR ", "or ").replace("MIN", "min").replace("MAX", "max").replace("AVG", "avg").replace("SUM", "sum")
	Queryops = QueryHandler(query)
	Queryops.read_meta_data()
	Queryops.parse()

if __name__ == '__main__':
    main()