"""Functions that can be used to solve problems in real life.

"""

from scipy.misc import comb

def divide_stakes(r, s):
    """Calculate the portions and ratio in the correct division of the stakes 
    for an interrupted game.
    
    The problem concerns a game of chance with two players who have equal 
    chances of winning each round. The players contribute equally to a prize 
    pot, and agree in advance that the first player to have won a certain 
    number of rounds will collect the entire prize. Now suppose that the game 
    is interrupted by external circumstances before either player has achieved 
    victory. How does one then divide the pot fairly? It is tacitly understood 
    that the division should depend somehow on the number of rounds won by 
    each player, such that a player who is close to winning will get a larger 
    part of the pot. But the problem is not merely one of calculation; it also 
    includes deciding what a "fair" division should mean in the first place.
    
    The starting insight for Pascal and Fermat was that the division should 
    not depend so much on the history of the part of the interrupted game that 
    actually took place, as on the possible ways the game might have 
    continued, were it not interrupted. It is intuitively clear that a player 
    with a 7-5 lead in a game to 10 has the same chance of eventually winning 
    as a player with a 17-15 lead in a game to 20, and Pascal and Fermat 
    therefore thought that interruption in either of the two situations ought 
    to lead to the same division of the stakes. In other words, what is 
    important is not the number of rounds each player has won yet, but the 
    number of rounds each player still needs to win in order to achieve 
    overall victory.
    
    Pascal finally showed that in a game where one player needs r points to 
    win and the other needs s points to win, the correct division of the 
    stakes is in the ratio of (using modern notation)
        s-1     /         \              r+s-1   /         \
        ----   / r + s - 1 \             ----   / r + s - 1 \
        \     |            |    to       \     |            | 
        /     |            |             /     |            |
        ----  \     k     /              ----  \     k     /
        k=0    \         /               k=s    \         /
    
    Parameters:
        r: the number of rounds player A still needs to win the game
        s: the number of rounds player B still needs to win the game
    
    Return:
        A list of three numbers in order: 
            portion for player A, 
            portion for player B, 
            ratio of the division for A and B, 
            the combination number for A, 
            the combination number for B
    
    """
    
    a = sum([comb(r + s - 1, k, exact=True) for k in range(0, s)])
    b = sum([comb(r + s - 1, k, exact=True) for k in range(s, r + s)])
    return([a/(a+b), b/(a+b), a/b, a, b])

