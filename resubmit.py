import random
import itertools
import paramiko
from time import sleep

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
	ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("qstat")

	output = ssh_stdout.readlines()
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
		ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("qstat -f {}".format (job[0]))
		jobInfo = ssh_stdout.readlines ()
		jobInfo = "".join (jobInfo)
		jobInfo = removeDuplicateSpaces (jobInfo)
		jobInfo = cutString (jobInfo, "Output_Path = aqua:", "napss.log")
		jobInfo = jobInfo.replace ("\n", "")
		jobInfo = removeSpaces (jobInfo)
		returnList.append (jobInfo)

	return returnList

def resubmitJobs (ssh, credentials, finishedJobs):
	usersList = list (credentials.keys ())
	cred2 = list (credentials.values ())
	loginList = []
	passwordList = []
	for creds in cred2:
		loginList.append (creds [0])
		passwordList.append (creds [1])

	for jobs in finishedJobs:
		for login in loginList:
			if (login in jobs):
				ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("cd {}; cat rg.dump >> rg_old.dump; cat S_Na.rdf >> S_Na_old.rdf; cat O_Na.rdf >> O_Na_old.rdf; cat CH_Na.rdf >> CH_Na_old.rdf; cat CH2_Na.rdf >> CH2_Na_old.rdf; cat Na.msd >> Na_old.msd; cat S_Na.groupgroup >> S_Na_old.groupgroup; cat O_Na.groupgroup >> O_Na_old.groupgroup; cat S_Na_kspace.groupgroup >> S_Na_kspace_old.groupgroup; cat O_Na_kspace.groupgroup >> O_Na_kspace_old.groupgroup; qsub quickRestarts.cmd".format (jobs))
				print ("job re-submitted in the location: {}".format (jobs))

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
		if (len (jobList_old) == 0):
			jobList_old = jobList

		jobDifference = difference_list (jobList_old, jobList)
		finishedJobs = []

		for job in jobDifference:
			if job not in jobList:
				finishedJobs.append (job)

		# Print recently finished jobs
		if (len (finishedJobs) > 0):
			print ("Job completed in the location: {}".format (finishedJobs))

		# If there are finished jobs, then resubmit them
		if (len (finishedJobs) > 0):
			resubmitJobs (ssh, credentials, finishedJobs)

		# TO DO: Make this sleep timer random, to prevent IP/account ban
		randomWaitTime = (int) (random.random () * 240)
		print ("Current sleep time: {}".format (randomWaitTime))
		sleep (randomWaitTime)

		# Random logout and login from session, with probability of 0.05
		if (random.random () < 0.05):
			ssh.close ()
			print ("logged out!")
			randomWaitTime = (int) (random.random () * 60)
			sleep (randomWaitTime)
			print ("logging in...")
			ssh.connect(ip_hpce, username = credentials ["raghurame"][0], password = credentials ["raghurame"][1], look_for_keys = False)
			randomWaitTime = (int) (random.random () * 60)
			sleep (randomWaitTime)

	# Close the ssh connection once the while loop ends
	ssh.close ()

if __name__ == '__main__':
	main ()
