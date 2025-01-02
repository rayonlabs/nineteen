<div align="center">

# **👀 nineteen [τ, τ] SN19**
Giving access to Bittensor with Decentralized subnet inference at scale. 

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[Bittensor](https://bittensor.com/whitepaper)  •  [Discord](https://discord.gg/dR865yTPaZ) • [SN19](https://sn19.ai)  • [SN19 APP](https://app.sn19.ai)
</div>


# Subnet 19 👀
Subnet 19 is decentralised inference of AI models. We call ourselves `Nineteen 👀`. Bittensor has the Nineteen of being the most powerful force in AI, powering the world through decentralisation. SN19 is all about fulfilling that `Nineteen` by getting decentralised AI into the hands of all.

We have a focus on inference, currently offering access to the best open source LLM's, Image generation models (including those trained on datasets from subnet 19), and other miscellaneous models such as embedding models. We've even home grown and open sourced our own workflows, such as avatar generation.

# Decentralisation
Nothing is more important than decentralisation -  we must be decentralised. We rely on no centralised API's - the miners' and validators' all run their own models. No single provider can bring us down.
 
## Decentralised miners
Miners all run open source models on any hardware they choose. They're incentivised by three things:
- Volume (amount of tokens, for example)
- Speed of requests (Measured in Tokens/s, for example)
- Quality of requests (Measured by accuracy compared to base model)

They're incentivised over a range of tasks, independently of each other. Miners have the option of choosing which tasks they want to run, and the volume they want to run for each. The more volume they have, the more rewards they get!

## Decentralised Validators
Validators operate as decentralised access points to the network. In a one-click fashion, Validators can offer up their miner access to the world, with a clear path to monetisation. 

They check and score the miners through 'Checking servers' that they run themselves on their own hardware. No centralisation to be found here.

## Organic scoring is a primary citizen
All organic requests can be scored, and checked for the upmost quality. Not only that, but we always keep track of the miners who are most likely to be available to answer organic requests, meaning the lowest latency and highest reliability possible! The user experience is the most important thing about SN19 - allowing validators to monetise and the world to experience Bittensor.

# Installation
### [Miners](docs/mining.md)

### [Validators](docs/validating.md)

## Latest release:
[See changelog here](changelog.md)


# Deep dive into the subnet mechanism
The subnet is split into `tasks`. These `tasks` are things like:
- Llama3.1 70B text generation [1]
- Stable Diffusion text to image generation [2]
  
Each of these independent tasks have a weighting associated with them (say [1]: 60%, [2]: 40%, for example ). Note they sum to 100%!

First, miners configure their servers and associate a specific capacity or volume for the task. Validators then fetch the capacities that each miner can handle for that particular task. Every 60 minutes (this interval is configurable), validators perform several critical steps for each miner.

Validators begin by determining the percentage of the miner's capacity they will test. They estimate how many queries are needed during the scoring period to test this capacity accurately. These queries are sent at regular intervals, and validators keep track of the results for quality scoring. They carefully note any failed queries or instances where miners rate limit the validator.

At the end of the scoring period, validators calculate a ﻿period score for each miner. This score reflects the percentage of requests that were correctly responded to. Miners are penalized for missed queries based on the volume left unqueried. If the announced volume is fully utilized, no penalties are imposed. However, if a miner is queried only once or twice and fails to respond, the penalties are more severe.

Simultaneously, validators assess the quality of all responses from fiber.src.miners, maintaining a running tally of their 'quality scores' and 'speed factors' (rewarding both speed and correctness). When it is time to set weights, validators calculate a volume-weighted average period score for each miner. This score is then multiplied by the miner's combined quality score to derive an overall score for that specific task.

Finally, the overall scores for each miner are combined across all tasks - which is what makes the tasks completely optional for miners. The different scores for all tasks are then summed up to derive a final overall score for each miner, which is used to set weights appropriately.

Phew!

### Ok that's a lot of text, what does it mean?
It means a few things:
- Miners can run tasks completely optionally. The more tasks they run, with greater volumes and speeds, the more incentive they will get.
- By controlling the reward distribution to miners, we can directly incentivise greater volumes and speeds for tasks that get greater organic usage. We could even give power for validators to have a stake weighted 'vote' for the tasks they care about...
- We can add as many tasks as we like! If there's demand for something we can add it! If not, we can remove it! No registrations or deregistrations needed, miners can just scale up and scale down their capacity as needed.
- Miners have the ability to rate limit explicitly to validators without incurring a greater penalty. This means we can much more effectively load balance between miners, to make sure any organic requests can be always handled by a miner who is capable of performing that task!
- There's nothing special about a synthetic or organic query. No distinction can be made on the miner side, but validators can still give preferential treatment to organics!
 
