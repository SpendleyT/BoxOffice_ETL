# Box Office Data for Analysis - ETL

<h3 style="text-decoration:underline;">Overview</h3>

<p>Have you ever asked yourself these questions:</p>
<ul>
 <li>Why does a movie get released at a certain time of year?</li>
 <li>What do people mean with the terms "Summer Blockbuster", "Awards Season" and "Dumpuary"?</li>
 <li>Why don't they make them like they used to?</li>
</ul>
<p>The goal of this project is to collect movie data (including release date, weekly ranking, total sales, and genre) and store it as raw data, clean the data to a relevant and reportable format, and load the data inro a data warehouse (PostgreSQL). The source for this data is Box Office Mojo by IMDB Pro<sup>TM</sup>, and will be retrieved utilizing the python library BoxOffice-API.</p>

<h3 style="text-decoration:underline;">The ETL Process</h3>
<h4>Extract</h4>
<p>(Describe the Extract process here)</p>

<h4>Transform</h4>
<p>(Describe the Transform process here)</p>

<h4>Load</h4>
<p>(Describe the Load process here)</p>


<h3 style="text-decoration:underline;">Installation Guide</h3>
<h4>Dependencies</h4>
<p>You will need the following to recreate this project:</p>
<ul>
<li>An Amazon Web Services account (for the s3 bucket)</li>
<li>An installation of PostrgeSQL</li>
<li>An OMDB API access_key (see <a href="https://www.omdbapi.com/apikey.aspx">omdbapi.com</a> for details). This is necessary for genre and other details.</li>
</ul>

<h4>Packages</h4>
<p>The following packages will be needed:</p>
<ul>
<li>Python (here: 3.11)</li>
<li>BoxOffice-API (found <a href="https://pypi.org/project/boxoffice-api/">here</a>.)</li>
<li>Boto3 (here: 1.28.64)</li>
<li>Pandas (here: 2.1.4)</li>
<li>SQL Alchemy (here: 1.4.52)
</ul>

<h4>Environment Variables</h4>
<p>The following environment keys are referenced in the code:</p>
</ul>
<li>AWS_ACCESS_KEY_ID - Your AWS account access key</li>
<li>AWS_SECRET_ACCESS_KEY - Your AWS account secret key</li>
<li>OMDB_KEY -  Your OMDB API credentials 
</ul>

<h4>Instructions</h4>





