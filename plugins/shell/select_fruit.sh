#!/bin/bash

FRUIT=$1

if [ $FRUIT == APPLE  ]; then
	echo "You selected Apple!"
elif [ $FRUIT == ORANGE ]; then
	echo "You selected Orange!"
elif [ $FRUIT == GRAPE ]; then
	echo "You selected GRAPE!"
else
	echo "YOU selected other fruit!"
fi	
