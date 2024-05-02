# Box Office Data for Analysis - ETL, API and Reporting

<h3>Overview</h3>

<p>Have you ever asked yourself these questions:</p>
<ul>
 <li>Why a movie is released at a certain time of year?</li>
 <li>What do people mean with the terms "Summer Blockbuster", "Awards Season" and "Dumpuary"?</li>
 <li>Why don't they make them like they used to?</li>
</ul>
<p>The goal of this project is to collect movie data (including release date, weekly ranking, total sales, and genre) and determine what information may be driving these decisions. The source for this data is Box Office Mojo by IMDB ProTM, and will be retrieved utilizing the python library BoxOffice-API.</p>

<h3>The ETL Process</h3>
<p>(Describe the ETL process here)</p>

<h3>Creating the API</h3>
<p>(Describe the flask api process here)</p>

<h3>Reporting/Analysis</h3>
<p>(Describe the reporting/Tableau here)</p>


<h3>Installation Guide</h3>
<h5>Dependencies</h5>
<p>You will need the following to recreate this project:</p>
<ul>
<li>An Amazon Web Services account (for the s3 bucket)</li>
<li>An installation of PostrgeSQL</li>
<li>An OMDB API access_key (see <a href="https://www.omdbapi.com/apikey.aspx">omdbapi.com</a> for details). This is necessary for genre and other details.</li>
</ul>

<h5>Packages</h5>
<p>The following packages will be needed:</p>
<ul>
<li>Python (here: 3.11)</ul>
<li>BoxOffice-API (found <a href="https://pypi.org/project/boxoffice-api/">here</a>.)</li>
<li>Flask (here: 2.2.5)</li>
<li>Boto3 (here: 1.28.64)</li>
<li>Pandas (here: 2.1.4)</li>
</ul>

<h5>Environment Variables</h5>
<p>The following environment keys are referenced in the code:</p>
</ul>
<li>AWS_ACCESS_KEY_ID - Your AWS account access key</li>
<li>AWS_SECRET_ACCESS_KEY - Your AWS account secret key</li>
<li>OMDB_KEY -  Your OMDB API credentials 
</ul>

<h5>Instructions</h5>





