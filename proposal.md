# Beyond the Spread: AI-powered Insights for NBA Betting

## Team Members and Github ids

### Github ids:

- Archie Frank - SYR-Archie
- Bryce Anthony (Point of contact)- bryce-ka
- Mervin McDouall - mervinmcdougall
- John Miller - jimiller00
- Michael A. d'Amore - madamore-1

## Background
Basketball, and the professional league run by the NBA, is the second most popular sport in the United States. According to Forbes, regular season NBA games average 1.4 million to 1.5 million viewers per game, with playoff games averaging 5.4 million viewers per game, and peaking at 11.65 million for the finals. Parallel to this, the sports betting industry has exploded since the historic ruling in Murphy v. NCAA, which opened up the possibility of state-regulated sports betting. As of 2023, sports betting is legal in 37 states and the District of Columbia. In 2022 alone, more than $93 billion in legal bets were placed in the US. Estimates for the total amount of legal bets placed on the NBA range from $10-20 billion.

Sports betting for the NBA takes three primary forms: betting on the winner of a game, the total number of points scored in a game, or betting on what is called “The Spread”. Odds for betting on the winner are given as either a numerical multiplier for the bettor’s return, or in terms of returns in $100 increments. For example, a bet could be at 1.25 odds, meaning that the return is $25 for a bet of $100. Or the bet could be placed at $130, meaning that the bettor would need to wager $130 to win $100. For the Spread, the bettor places a stake on a positive or negative number representing a difference between the final scores of the competing teams. If a team’s value is negative, the team would need to win by more than the negative value to win the bet. If the value is positive, the team would need to lose by less than the value or win the game.

## Introduction
  
Our team intends to create a deep learning model to predict the winning and losing team of an NBA game, as well as the difference between the team scores. Our approach to solving this problem involves taking customizable inputs to keep up with the ever-changing conditions leading up to a given game. The model can be adjusted to account for players who are unavailable due to penalty, injury, or sickness, and other factors based on further exploration of the data.

The specifics of how bookmakers set their odds are closely guarded secrets – bookmakers consider a variety of factors ranging from past performance to home court advantage when setting their odds. Trying to predict the outcome of a sporting event isn’t novel or new, even with the advent of machine learning. However, robust prediction algorithms are not accessible for individuals, and require specialized skills to build, train, and maintain. 

Our end goal is to provide AI-backed insights into the outcomes of NBA sporting events to anyone. We believe that giving users access to the predictive power of machine learning can help enthusiasts, analysts, pundits, and casual observers better understand a single game and how it fits into the season as a whole. Armed with this knowledge, they can then make predictions with greater confidence – on or off the books.

  
## Literature Review
The rapid growth and profitability of the sports betting industry have attracted significant research interest. As a consequence, a substantial body of research exploring the application of artificial intelligence (AI) in sports emphasizes the profitability of AI models as a key measure of success. While this has led to a variety of approaches, making direct comparisons between models is challenging. Many studies incorporate specific betting strategies within their models, making it difficult to isolate the unique contribution of the model itself to generating the profitable outcomes. Additionally, the focus of our project departs from this profit-driven approach as we aim to investigate how accurately AI can be used to obtain the key information behind the decision-making in sports betting, rather than focusing on the specific betting decision itself. 

Studies investigating the prediction of game outcomes and final score differentials have converged on two key findings: the critical role of feature engineering and the superior performance of neural network architectures for this task. *Cheng et al[^1]*. and *Zhao et al[^2].*, focused on the final score differentials, found that the neural networks performed better than the machine learning algorithms tested. Similarly; *Boll[^3]*, focused on predicting the final score differentials, found that even a simple neural network using feature-engineered inputs had similar performance to the highest performing machine learning algorithms.

The importance of feature engineering was echoed in all the other studies as well. While all the studies found that correlation based methods of feature selection improved the performance of their neural networks. More creative approaches have been more fruitful.  *Zhao et al[^2].*, focused on match outcome prediction- found a model implementing a combination of input features altered using a fuzzy logic system and a neural network to be the highest performing model in their literature review. Similarly, *Oksen et al[^4].* attempted to incorporate player synergies through feature engineering and achieved an accuracy of 76% when predicting the outcome of matches. *Bucquet et al[^5].*, predicting the total combined points scored in a game, tested a variety of AI based approaches and found a feed forward neural network to perform better than a LSTM.

Focusing on the application of neural networks for the match outcome and point differential prediction; the variety of available approaches, lack of player data used in models, various neural network architectures, and lack of consensus amongst previous researchers leaves plenty to be explored. Since we intend for our work to be used to provide key information behind the decision-making in NBA sports betting, its important for us to be transparent about the strengths and weaknesses of our model when it is shown to individuals outside our team. 


## Data and Methods

### Data
We plan to utilize NBA data sourced through a collection of NBA APIs, the official NBA statistics reference, and Basketball Reference. The dataset includes seasonal data from 1996 to present, for all 82 regular games played by each of the 30 teams in the NBA and subsequent playoff appearances. Each team has a roster of 15 to 18 total players across a season. There are team-wide performance stats for every game as well as individual player performance. We will connect these statistics with othe features such as the Player Efficience Rating (PER), game location for example if a team is playing a home game, referee information and other features to help improve the accuracy of the output from the model.

The data is from a verified source – and is provided by the governing body of the NBA itself. The data is in a normalized format, but will be adjusted following the completion of our  exploratory analysis.

### Methods
Building off of previous research, we intend to explore the effectivness of neural networks and feature engineering for this match outcome and point differential prediction. Although we are still researching various methods of feature engineering for tasks involving the NBA,  based on our research- the inclusion of player data in our models is likely to have a meaningful impact on the effectivness of our model. 

## Stakeholders
The ability to predict the outcome of a basketball game, and predict the amount of points a team will win by, is lucrative and useful to many people. The stakeholders with highest interest in this predictive model are sports bettors. Their motivation for using this model will be to minimize risk and improve the odds of winning, especially for a spread which is made to be as close to 50/50 odds as possible. Other interested parties include sports analysts, hardcore NBA fans, and casual observers who want to improve their analysis of the game. Content creators covering the NBA who talk about games and their outcomes could make better predictions, therefore making their analysis better and increasing their ability to analyze the game. Also, NBA fans who want to know more about the game they are passionate about may use it casually.

## Busines model
Although the primary goal is to get a functional model which can provide better betting odds, the  team has considered some viable commercial options for this project. One option is a subscription-based  model which is available to both individuals and organizations with different tiers of pricing based on frequency of requests. Alternatively, the model can be pitched to specific betting organizations to be sold outright for a designated price.


## Communication
Our team uses MS teams to communicate via virtual meetings and chat. We plan to use github to share and collaborate on coding, and to hold our dataset.


## Project Plan
Period|Activity|Milestone
|---|---|---|
| 03/02/2024 | Collect data via API and webscraping | Ensure we have a working API which we can use for collecting data. |
| 03/09/2024 | Identify target features | Ensure we have sufficient data to make prediction. We may have to combine datasets from alternate sources. Therefore, we should ensure we have all or more than the data we need. |
 | 03/16/2024 | Exploratory analysis | Ensure we have identified features to drop, features that are relevant and should be kept, correlations, and targets. |
| Format and clean data | See below | |
| 03/23/2024 | Identify best model type | Cleaning and formatting and identifying the best model type can likely be done in one go. Once we have created a pipeline for preprocessing, we can share this pipeline among group members for testing models. |
| Train model | | |
| 03/30/2024 | Optimize model | Training and optimizing can possibly be done together or at least after. A fallback has been provided for any carry-over work. |
| 04/13/2024 | Test model | Two weeks are being provided for testing. However, the target is the first week. |
| 04/20/2024 | Fallback | |


## Risks
The biggest risk to our product would be the misinterpretation of the generated results. It needs to be clearly communicated with any and all users that these are only predictions, and not guaranteed outcomes. Any predictions given would need to be placed behind a disclaimer and user-agreement that removes liability. It should also be noted that the model does not and cannot taken into account conditions that are beyond the scope of the data. For example, predicting the outcome of games during a pandemic or natural disaster.


Another risk which is directly correlated to game use is that of gambling addiction. Any end user license or disclaimer which is presented to the end user should include legal information regarding gambling addiction and where one may seek assistance.


[^1]: [Predicting the Betting Line in NBA Games](https://cs229.stanford.edu/proj2013/ChengDadeLipmanMills-PredictingTheBettingLineInNBAGames.pdf)

[^2]: [Enhancing Basketball Game Outcome Prediction through Fused Graph Convolutional Networks and Random Forest Algorithm](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10217531/#:~:text=Cao%20%5B25%5D%20used%20machine%20learning,a%20success%20rate%20of%2069.67%25./#entropy-25-00765-t001)

[^3]: [Gridiron Genius: Using Neural Networks to Predict College Football Games](https://deepblue.lib.umich.edu/bitstream/handle/2027.42/176935/Luke_boll_capstone_poster_-_Luke_Boll.pdf?sequence=2)

[^4]: [Predicting the winning team in basketball: A novel approach](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9764182/#entropy-25-00765-t001)

[^5]: [The Bank is Open: AI in Sports Gambling](https://cs229.stanford.edu/proj2018/report/3.pdf)
