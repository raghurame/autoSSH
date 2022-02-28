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

def takeBackup (ssh, jobs, userDirName):
	backupLocations = {"T286": "/home/raghuram/Documents/HPCE_backups/T286/hbondLifetime/", "T298": "/home/raghuram/Documents/HPCE_backups/T298/hbondLifetime/", "T310": "/home/raghuram/Documents/HPCE_backups/T310/hbondLifetime/", "T323": "/home/raghuram/Documents/HPCE_backups/T323/hbondLifetime/", "T335": "/home/raghuram/Documents/HPCE_backups/T335/hbondLifetime/", "T348": "/home/raghuram/Documents/HPCE_backups/T348/hbondLifetime/"}

	for temperature in backupLocations:
		if temperature in jobs:
			targetLocation = backupLocations[temperature]
			print ("Target location set as: {}".format (targetLocation))

	print ("Copying files from HPCE")
	os.system (" sshpass -p \"swathi@123\" rsync --remove-source-files --progress -avz {}@10.24.6.200:{}/dump_nvt.lammpstrj {}/dump_new.lammpstrj".format (userDirName, jobs, targetLocation))
	print ("Appending temp file to main traj file...")
	os.system ("cat {}/dump_new.lammpstrj >> {}/dump_nvt.lammpstrj &".format (targetLocation))

def checkOccupiedSpace (ssh, userDirName):
	ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command ("cd; cd ../; du {}/ -sh".format (userDirName))
	readSSHOutput = ssh_stdout.readlines ()
	return float(readSSHOutput[0].replace("G\t{}/\n".format (userDirName), ""))

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
				while (success == 0):
					try:
						currentOccupiedSpace = checkOccupiedSpace (ssh, "mm16d002")
						print ("Occupied space: {}".format (currentOccupiedSpace))
						if (currentOccupiedSpace > 40.0):
							print ("Available space is very low in the current account. Preparing to take backup for {}".format (jobs))
							takeBackup (ssh, jobs, "mm16d002")
						ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd {}; qsub quickRestarts.cmd".format (jobs))
						success = 1
					except:
						success = 0
						ssh.close ()
						print ("logged out because job submission failed...")
						print ("logging in again...")
						try:
							ssh.connect(ip_hpce, username = credentials ["swathi"][0], password = credentials ["swathi"][1], look_for_keys = False)
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
	ssh.connect(ip_hpce, username = credentials ["swathi"][0], password = credentials ["swathi"][1], look_for_keys = False)

	# Iterate in while loop forever
	while (1):
		# Create a duplicate jobList to compare
		jobList_old = jobList
		jobList = checkQueues (ssh, usernames)
		print ("Current jobs:")
		for job in jobList:
			if (len (job) > 5):
				print (job)

		# Vaild during the first 'while' loop run
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
				ssh.connect(ip_hpce, username = credentials ["swathi"][0], password = credentials ["swathi"][1], look_for_keys = False)
			except Exception as e:
				pass
			randomWaitTime = (int) (random.random () * 60)
			sleep (randomWaitTime)

	# Close the ssh connection once the while loop ends
	ssh.close ()

if __name__ == '__main__':
	main ()
