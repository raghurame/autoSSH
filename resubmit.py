import datetime
import random
import itertools
import paramiko
from time import sleep
import sys
import os

def cutString(fullString, subString1, subString2):
	index1 = fullString.find (subString1)
	index1 = index1 + len (subString1)
	index2 = fullString[index1:].find (subString2)
	return (fullString[index1:index1 + index2])

def removeSpaces (inputString):
	while (" " in inputString):
		inputString = inputString.replace ("  ", "")
	inputString = inputString.replace ("\t", "")
	return inputString

def removeDuplicateSpaces (inputString):
	while ("  " in inputString):
		inputString = inputString.replace ("  ", " ")
	return inputString

def difference_list(list1, list2):
	list_difference = [i for i in list1 + list2 if i not in list1 or i not in list2]
	return list_difference

def checkQueues (ssh, usernames):

	try:
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("qstat")
	except Exception as e:
		pass

	try:
		output = ssh_stdout.readlines()
	except Exception as e:
		output = ""
		
	jobList = []
	returnList = []

	for line in output:
		if (".hn1" in line):
			line = removeDuplicateSpaces (line)
			line = line.replace (" ", ",")
			lineArray = line.split (",")
			if (lineArray [5].lower () == 'small8'):
				jobList.append (lineArray [:4])

	for job in jobList:
		try:
			ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("qstat -f {}".format (job[0]))
		except Exception as e:
			pass

		try:
			jobInfo = ssh_stdout.readlines ()
		except Exception as e:
			jobInfo = ""

		jobInfo = "".join (jobInfo)
		jobInfo = jobInfo.replace ("\n", "").replace ("\t", "")
		jobInfo = removeDuplicateSpaces (jobInfo)
		jobInfo = cutString (jobInfo, "Output_Path = aqua:", "n10.log")
		jobInfo = removeSpaces (jobInfo)
		returnList.append (jobInfo)

	return returnList

def takeBackup (jobLocation):
	# reading config file
	with open ("backup.config", "r") as file:
		for line in file:
			HPCE_dir, local_dir = line.split (" ")
			if (jobLocation == HPCE_dir):
				# os.system (" sshpass -p \"raghurame@123\" rsync --remove-source-files --progress -avz ic34946@10.24.6.200:{}pairLocal/ {}pairLocal/".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --remove-source-files --progress -avz ic34946@10.24.6.200:{}dump_solvent1.lammpstrj {}dump_solvent.lammpstrj".format (HPCE_dir, local_dir))
				os.system (" cat {}dump_solvent.lammpstrj >> {}dump_solvent_full.lammpstrj".format (HPCE_dir, local_dir))
				os.system (" rm {}dump_solvent.lammpstrj".format (local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}run.restart {}".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rg/rg_old.dump {}rg/rg_old.dump".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/S_Na_old.rdf {}rdf/S_Na_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/O_Na_old.rdf {}rdf/O_Na_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/CH_Na_old.rdf {}rdf/CH_Na_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/CH2_Na_old.rdf {}rdf/CH2_Na_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/S_H2O_old.rdf {}rdf/S_H2O_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}rdf/O_H2O_old.rdf {}rdf/O_H2O_old.rdf".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}msd/cation_old.msd {}msd/cation_old.msd".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}msd/anion_old.msd {}msd/anion_old.msd".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}msd/water_old.msd {}msd/water_old.msd".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}groupgroup/O_Na_old.groupgroup {}groupgroup/O_Na_old.groupgroup".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}groupgroup/O_Na_kspace_old.groupgroup {}groupgroup/O_Na_kspace_old.groupgroup".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}groupgroup/O_W_old.groupgroup {}groupgroup/O_W_old.groupgroup".format (HPCE_dir, local_dir))
				os.system (" sshpass -p \"raghurame@123\" rsync --progress -avz ic34946@10.24.6.200:{}groupgroup/O_W_kspace_old.groupgroup {}groupgroup/O_W_kspace_old.groupgroup".format (HPCE_dir, local_dir))

	# os.system (" sshpass -p \"raghurame@123\" rsync --remove-source-files --progress -avz ic34946@10.24.6.200:{}pairLocal/ {}pairLocal/".format (jobLocation))
	# os.system (" sshpass -p \"raghurame@123\" rsync --remove-source-files --progress -avz ic34946@10.24.6.200:{}dump_solvent.lammpstrj {}dump_solvent.lammpstrj".format (jobLocation))
	# os.system (" cat {}dump_solvent.lammpstrj >> {}dump_solvent_full.lammpstrj")
	# os.system (" rm {}dump_solvent.lammpstrj")

def resubmitJobs (ssh, credentials, finishedJobs):
	ip_hpce = "10.24.6.200"
	print ("Entered resubmitJobs function")
	success = 0
	usersList = list (credentials.keys ())
	cred2 = list (credentials.values ())
	loginList = []
	passwordList = []
	for creds in cred2:
		loginList.append (creds [0])
		passwordList.append (creds [1])

	for jobs in finishedJobs:
		success = 0
		for login in loginList:
			if (login in jobs):

				# Creating a LAMMPSTRJ file with last frame
				# This last frame won't be moved to local machine
				# Simulation can also be restarted using this last saved timeframe using read_dump command
				# while (success == 0):
				# 	try:
				# 		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd {}; ./saveLastFrame.sh".format (jobs))
				# 		success = 1
				# 	except:
				# 		success = 0
				# 		ssh.close ()
				# 		print ("logged out because job submission failed...")
				# 		print ("logging in again...")
				# 		try:
				# 			ssh.connect(ip_hpce, username = credentials ["raghurame"][0], password = credentials ["raghurame"][1], look_for_keys = False)
				# 		except Exception as e:
				# 			pass

				# # Taking backups before submitting the next set of simulations
				# takeBackup (jobs)

				# Submitting the completed simulation again
				while (success == 0):
					try:
						ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd {}; qsub quickRestarts.cmd".format (jobs))
						success = 1
					except:
						success = 0
						ssh.close ()
						print ("logged out because job submission failed...")
						print ("logging in again...")
						try:
							ssh.connect(ip_hpce, username = credentials ["raghurame"][0], password = credentials ["raghurame"][1], look_for_keys = False)
						except Exception as e:
							pass

				# now = datetime.datetime.now()
				now = datetime.datetime.utcnow () + datetime.timedelta (hours = 0, minutes = 0)
				print ("job re-submitted in the location: {} (qsub quickRestarts.cmd) ({})".format (jobs, str (now)))

def main ():
	credentials = {"raghurame": ["ic34946", "raghurame@123"], "vidhya": ["ic34784", "vidhya@123"], "rinsha": ["mm18d016", "rinsha@123"], "swathi": ["mm16d002", "swathi@123"], "santhra": ["mm21d009", "santhra@123"], "aneela": ["mm21d404", "aneela@123"], "subrata": ["mm21s003", "subrata@123"], "saiteja": ["mm21s014", "saiteja@123"]}
	usernames = ['ic34784', 'ic34946', 'mm18d016', 'mm16d002', 'mm21d009', 'mm21d404', 'mm21s003', 'mm21s014']

	jobList_old = []
	jobList = []

	# ssh init
	ip_hpce = "10.24.6.200"
	ssh = paramiko.SSHClient()
	ssh.load_system_host_keys()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

	# ssh login
	ssh.connect(ip_hpce, username = credentials ["raghurame"][0], password = credentials ["raghurame"][1], look_for_keys = False)

	# Iterate in while loop forever
	while (1):
		# Create a duplicate jobList to compare
		jobList_old = jobList
		jobList = checkQueues (ssh, usernames)
		print ("Current jobs:")
		print (jobList)
		if (len (jobList_old) == 0):
			jobList_old = jobList

		jobDifference = difference_list (jobList_old, jobList)
		finishedJobs = []

		for job in jobDifference:
			if job not in jobList:
				finishedJobs.append (job)

		# Print recently finished jobs
		if (len (finishedJobs) > 0):
			now = datetime.datetime.utcnow()+datetime.timedelta(hours = 0, minutes = 0)
			print ("Job completed in the location: {} ({})".format (finishedJobs, str (now)))
			resubmitJobs (ssh, credentials, finishedJobs)

		# TO DO: Make this sleep timer random, to prevent IP/account ban
		randomWaitTime = (int) (random.random () * 300)
		# now = datetime.datetime.now()
		now = datetime.datetime.utcnow()+datetime.timedelta(hours = 0, minutes = 0)
		print ("Current sleep time: {} s ({})".format (randomWaitTime, str (now)))
		sleep (randomWaitTime)

		# Random logout and login from session, with probability of 0.05
		if (random.random () < 0.05):
			ssh.close ()
			print ("logged out!")
			randomWaitTime = (int) (random.random () * 1200)
			print ("Current sleep time: {} s ({})".format (randomWaitTime, str (now)))
			sleep (randomWaitTime)
			print ("logging in...")
			try:
				ssh.connect(ip_hpce, username = credentials ["raghurame"][0], password = credentials ["raghurame"][1], look_for_keys = False)
			except Exception as e:
				pass
			randomWaitTime = (int) (random.random () * 60)
			sleep (randomWaitTime)

	# Close the ssh connection once the while loop ends
	ssh.close ()

if __name__ == '__main__':
	main ()
