# Reflection — Prattasha Barua

## My Area
The dashboard and privacy protection in published outputs.

## How It Works, in My Own Words

**Question 1: There are five charts. What happens when someone clicks on a district in one chart?**

When someone clicks on a district in the dashboard, the shared selection is updated. The other views respond by highlighting or filtering the selected district, making it easier to explore and understand the data.

**Question 2: If a district has 1–4 patients, why don't we show the exact number?**

We hide counts between 1 and 4 to protect patient privacy and prevent individuals from being identified. This technique is called small-cell suppression and helps keep sensitive health information confidential.

## A Decision and Why

We show "withheld" instead of zero in hidden cells.

**Question 3: What would viewers misunderstand if we showed zero?**

Viewers might think that there were no patients in that district, even though there were actually 1–4 cases. Showing "withheld" makes it clear that the number is hidden for privacy reasons, not that there were zero cases.

## What I Learned

**Question 4: What new things did you learn?**

I learned how to build interactive dashboards using D3.js and how different charts can share the same selection state. I also learned about small-cell suppression and complementary suppression, which help protect patient privacy when publishing sensitive health data.
