# Hi and welcome.

My name is **Demian Vöhringer** and in the next 15 minutes i will present our research regarding **WATT, Write Aware Timestamp Tracking: Effective and Efficient Page Replacement for Modern Hardware**.

## For this paper we revisited the classical problem of Page Replacement

First we looked at **recent challenges of Page Replacement**

Storage moved from **HDDs to SSDs** because SSD offer a much better **Random read throughput**  
But they also have a **Read/Write Asymmetry** with slower writes than reads and their **Lifetime** is influenced by the amount of writes.  
Servers changed from **single core CPUs** to having dozens or hundreds of cores to allow parallelism.

From this we derived the **4 Goals of Page Replacements**

1. Replacement Effectiveness:  
  I fyou have 2 different replacement strategies A and B
  a is better than b if A produces **less misses or reads** than B.
2. Write Awareness:  
  a is better than b if A produces **less writes** than B.
3. CPU Efficiency:  
  a is better than b if A needs **less CPU** than B for **accesses and evictions**.
4. Multi-Core Scalability:  
  a is better than b if A **scales better** than B. %(Synchronization, ...)

So, with that, lets have a look at what **strategies** are **used for page replacement**

We can see that **most systems use LRU** or an adaptation of LRU  
LRU2, based on LRU-K from 1993 is used in SQL Server  
and Clock-Sweep, an approximation of LRU is used in PostgreSQL.

## We evaluated and compared 8 state of the art strategies and Random against those goals

As you can see, none of them fulfills our four goals.

## So let's talk about WATT

With watt we follow the idea that **more Information** leads to **more insights**, which can lead to a **better strategy**.

Therefore we start by **tracking every access** on a Page in a **Tracking Array**.

You can see, how on each access the **current timestamp is prepended** to the tracking array.
The first access happens at t=0, therefore we put 0 at the start of the array.
On the second access at t=8 we prepend 8 to the array and so on.

## So, how can we use that to evict a page?

For this we use the concept of Page Values.
A Page Value Function should give a **good page a high value** and a **bad one a low value** to decide, which page to evict next.
As nearly all strategies we would try to predict the next access happening.
One way to do this is by approximating the **current Access Frequency**.

We can see, how **LRU** uses the **age of the last access**, respectively the **most recent** access timestamp for a page value.  
**LRU2** does the same with the **next most recent** access timestamp.  
And finally **LFU** takes the **length** of the current Tracking Array.

### To use the information from the Tracking Array for WATT we introduce Subfrequencies

Subfrequencies in different orders are calculated by taking **the order** of that subfrequency and **dividing** it by the **time passed** since the **corresponding access**.

For example: for our **Subfrequency of first** order we divide 1 by the time passed since the **most recent** access.
For the **second order Subfrequency** we divide 2 by the time passed since the **next most recent** access.

Therefore we get something **commonly know as frequency: occurrences over time.**

You can see, how easily they are calculated with our example **Tracking Array from before**.
First we calculate the age of each access, and bye a simple devision we get the subfrequencies.

### To get a **single Page Value** we now have to **aggregate** those Subfrequencies to a single value.

Based on our **experiments** we chose the **maximum function** to be a good aggregator.

Therefore in our Example, our PageValue is the same as the first order Subfrequency.

### If we now look back at how the other Strategies calculate their PageValue

we see, they use the Subfrequency framework, too.
**LRU** just uses the **first** order Subfrequency,  
LRU2 uses the **second** order Subfrequency  
and LFU uses something we could call **Maximum Order** Subfrequency.

Watt combines all SF to one PV and is therefore able to use advantages of LRU, LRU2 and LFU.

### For WATT we had to solve some **additional problems**

- To add  **Write Awareness** we used **two** different **Tracking Arrays**, one for **Accesses**, and one **modifications**. For both we calculated **separate PageValues** and added them with a **weighted_sum** to **adopt** to different Environments.
- To limit memory consumption we limit the tracking of Access to 8, and the tracking of Modifications to 4.
- Because **Access Bursts** can **fill** the Tracking Array quite fast, we **group Accesses** to **slowly changing epochs** and **ignore** accesses from the **same epoch**.
- Like **LRU2, 2Q and Arc** add modifications to LRU to **handle Scans and OneTime access** better by giving single accesses or the **most recent access less weight**, we modified our PageValue by **Dampening the first order Subfrequency** to a tenth of its value.
- Because a PageValue offers **no data structure like LRU** to find the eviction candidate, we had to **random sample a few pages**, **evaluated** their **page values** to evicted the one with the **lowest page value**.

## Now after presenting the idea behind WATT we are now interested in how good WATT fulfills our four goals.

For this we wrote and published a **simulation framework**.

To check the **replacement effectiveness, our first goal**, we **compared** WATT **against 8 state of the art strategies and random** on **5 different Traces**.

You can see a selection here:
Each **little graph** shows, how a **strategy (the columns)** on a **specific trace (the rows)** compares against WATT on a **specific Buffer pool ratio**.
If the Graph is **below 0**, the other strategie produce **less misses than WATT**, marked in **red**, and if it is **above 0**, it produces **more Misses than WATT**, marked in **green**.

You can see, that, even if there are **some parts** where a specific strategy produces less misses than watt, **WATT overall generates less misses than any other strategy**.

### To check for **Write Awareness, our second goal**, we compared WATT against 9 other strategies, including 2 state of the art write aware strategies, **LRU-with Write Sequence Reordering** and **Clean First LRU**.

Because **Clean First LRU** and WATT both offer a **write weighting parameter**, we evaluated both of them with **multiple parameters**.

As you can see, the **left axis** shows the amount of **writes** and the **lower axis** the amount of **misses**.
Therefor a strategy in the **lower left corner** would be the **best**.

We can see, that WATT with its different write weights is better than any other strategy in comparison of Writes and misses.

## To evaluate **both other goals** we did a **System Evaluation**.

For this we **implemented** WATT together with **two highly scalable** eviction strategies **Hyberbolic Caching** and **Random** in **LeanStore**, a **high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs**.
Because it was already implemented, we added **LeanEvict**, the default replacement strategy of Leanstore to our comparison.

We implemented WATT using a **lock free access to the Tracking Array** and **SIMD-Vectorization** to calculate the Page Value.
As we use Random Sampling for Page Value based eviction and this can produce a lot of cache misses, we added prefetching to hide them.

### We used our System Evaluation to again compare Goals 1 and 2, Effectiveness and Write Awareness

With **WATT** as the **baseline**, you can see, that every other strategy **produces more reads and writes per Transaction**.

### CPU Efficiency, our third goal consists of **two parts**.

The **overhead** of **accessing** a page and of **evicting** a page.

For **accesses** we ran an **In-Memory workload** and compared a few **CPU-Measurements**, as presented here.
With **Random as our Baseline**, You can See that all Strategies use a **similar amount of CPU** and only **minor differences** appear.
Therefore on **accesses** WATT is as **CPU Efficient as Random**.

#### For **Evictions** we measured the **Time**, each strategy takes to **evict a specific amount of pages**.

Again you can see WATT being close to our **Baseline, the no overhead strategy Random**.
Therefore we can conclude, that **WATT is highly CPU efficient**.

#### For **Multi-Core Scalability, our fourth goal** we measured the **eviction capacity using multiple threads**.

On the **left** side we can see the performance on an **read only trace**. Therefore all pages are **clean** and can just be **dropped**.
Here **WATT** scales **as good as hyperbolic caching** and quite **similar to Random**.

On the **right** side we used a trace generating **dirty pages**.
Therefore a dirty page need to be **written out before evicting it**, adding **communication to the ssd**.
Here all four strategies scale **quite similar**.

Therefore we can conclude that **WATT is as scalable as Random** and therefore **highly scalable**.

#### Going bach to our **Comparison Matrix** we can see, that **Watt fulfills all four goals**.

and therefore should be considered as a **Replacement Strategy fit for modern Hardware**.

## In this talk we presented

- WATT: Write Aware Timestamp Tracking: Effective and Efficient Page Replacement for Modern Hardware.
- We presented **four goals** that page replacement strategies should fulfill to be **fit for modern hardware**, and showed how WATT accomplishes **all four goals**.
- The **paper** and a link to the repository containing the **Implementation in LeanStore**, the **Simulation Framework** and the **Traces** used, can be found on **leanstore.io**.

There are two other Leanstore Paper available at VLDB 2023:

- in slot D2: What Modern NVMe Storage Can Do, And How to Exploit it: High-Performance I/O for High-Performance Storage Engines
- and in Slot C8: Scalable and Robust Snapshot Isolation for High-Performance Storage Engines.

## Thank you for you time and interest.

I am now happy to take your questions.

## Appendix

- Space used
- Full Comparison
- PageTracker::track
- PageTracker::PVaccess
- Write Aware TPC-E
- Simulation Traces
- LeanStore Traces
