import datetime
import re

# recipe taken from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/145672
def formatBlock(block):
	'''Format the given block of text, trimming leading/trailing
	empty lines and any leading whitespace that is common to all lines.
	The purpose is to let us list a code block as a multiline,
	triple-quoted Python string, taking care of indentation concerns.'''
	# separate block into lines
	lines = str(block).split('\n')
	# remove leading/trailing empty lines
	while lines and not lines[0]:  del lines[0]
	while lines and not lines[-1]: del lines[-1]
	# look at first line to see how much indentation to trim
	ws = re.match(r'\s*',lines[0]).group(0)
	if ws:
			lines = map( lambda x: x.replace(ws,'',1), lines )
	# remove leading/trailing blank lines (after leading ws removal)
	# we do this again in case there were pure-whitespace lines
	while lines and not lines[0]:  del lines[0]
	while lines and not lines[-1]: del lines[-1]
	return '\n'.join(lines)+'\n'


def add_month(curr_date):
	'''
	Add a month to a given year. If the original month is december,
	change the month to january and add 1 year.
	'''
	new_year = curr_date.year
	new_month = curr_date.month + 1
	new_day = curr_date.day
	
	if new_month > 12:
		new_month = 1
		new_year += 1;
	
	return datetime.date(new_year, new_month, new_day)


def main():
	#statements
	create_table = ""
	create_index = ""
	create_function = ""
	create_trigger = ""
	
	#begin processing
	
	#holds current date
	t = datetime.date.today()

	#extract Year and month fom date, format for partition name
	pname = t.strftime('archive_y%Ym%m')
	
	#String to create a table
	create_table = formatBlock(
		"""
		CREATE TABLE %(pname)s (
			CHECK ( added >= DATE '%(now)s' AND added < DATE '%(next_month)s')
		) INHERITS (archive);
		""" % {
				'pname': pname,
				'now': t.strftime('%Y-%m-01'),
				'next_month': add_month(t).strftime('%Y-%m-01')
			}
		)
	
	create_index = "CREATE INDEX %(pname)s_added ON %(pname)s (added);\n"%{'pname':pname}
	
	create_function = formatBlock(
		"""
		CREATE OR REPLACE FUNCTION archive_insert_trigger()
		RETURNS TRIGGER AS
		$$
		BEGIN
			INSERT INTO %(pname)s VALUES (NEW.*);
			RETURN NULL;
		END;		
		$$
		LANGUAGE plpgsql;
		""" % {'pname': pname}
	)

	create_trigger = formatBlock(
		"""
		CREATE TRIGGER archive_insert_trigger
	   		BEFORE INSERT ON archive
	   		FOR EACH ROW EXECUTE PROCEDURE archive_insert_trigger();
		"""	
	)
	
	#partition file
	pf = file("new_partition.sql", "w");
	
	pf.write("-- for partitions be sure to use\n")
	pf.write("SET constraint_exclusion = on;\n\n")
	pf.write(create_table + "\n")
	pf.write(create_index + "\n")
	pf.write(create_function + "\n")
	pf.write(create_trigger + "\n")
	pf.write("-- INSERT INTO archive SELECT md5(filename), inode, uid, gid, size, atime, mtime, ctime FROM snapshot;\n")
	
	pf.close();

if __name__ == "__main__":
	main()
