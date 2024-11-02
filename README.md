HELPER FOR UPLOADING HUGE FILES INTO OPENAI'S BATCH API:

I'm sure many versions of this already exist in the ether I am also putting this into -- but I didn't look into it, paid for this in some hours of my life, and this was hence birthed.
So if a search engine brings you here instead of elsewhere -- this code can be run to:
1. Check that your file is below the token limit for gpt-4o-mini
2. Chunk if it's not
3. Wait for your other files to finish processing, before
4. Reattempting uploading

Just:
1. Put in your own OpenAI API Key
2. Replace input folder name

