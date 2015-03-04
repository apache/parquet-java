<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Committers (in aplhabetical order):

| Name               | Apache Id  | github id      | JIRA id     |
|--------------------|------------|----------------|-------------|
| Aniket Mokashi     | aniket486  | aniket486      |             |
| Brock Noland       | brock      | brockn         |             |
| Chris Aniszczyk    | caniszczyk |                |             |
| Dmitriy Ryaboy     | dvryaboy   | dvryaboy       |             |
| Jake Farrell       | jfarrell   |                |             |
| Jonathan Coveney   | jcoveney   | jcoveney       |             |
| Julien Le Dem      | julien     | julienledem    | julienledem |
| Lukas Nalezenec    | lukas      | lukasnalezenec |             |
| Marcel Kornacker   | marcel     |                |             |
| Mickael Lacour     | mlacour    | mickaellcr     |             |
| Nong Li            | nong       | nongli         |             |
| Remy Pecqueur      | rpecqueur  | Lordshinjo     |             |
| Ryan Blue          | blue       | rdblue         |             |
| Tianshuo Deng      | tianshuo   | tsdeng         |             |
| Tom White          | tomwhite   | tomwhite       |             |
| Wesley Graham Peck | wesleypeck | wesleypeck     |             |

Reviewing guidelines:
Committers have the responsibility to give constructive and timely feedback on the pull requests.
Anybody can give feedback on a pull request but only committers can merge it.

First things to look at in a Pull Request:
 - Is there a corresponding JIRA, and is it mentioned in the description? If not ask the contributor to make one.
 - If a JIRA is open, make sure it is assigned to the contributor. (they need to have the contributor role here: https://issues.apache.org/jira/plugins/servlet/project-config/PARQUET/roles)
 - Is it an uncontroversial change that looks good (has apropriate tests and the build is succesful)? => merge it
 - Is it something that requires the attention of committers with a specific expertise? => mention those committers by their github id in the pull request.
