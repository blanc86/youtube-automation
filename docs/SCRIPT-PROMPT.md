# The script prompt

You do not need an API key or any paid service to generate scripts. Paste the
prompt below into any chat assistant — Claude, ChatGPT, Gemini — add your plot
idea at the bottom, and paste the reply straight into a `.txt` file.

That file is what `ytauto project create --story` takes.

```bash
ytauto project create --slug my-video --title "My Video" --story script.txt
ytauto run --project my-video
```

---

## How to use it

1. Copy everything inside the box below.
2. Replace the two bracketed lines at the bottom with your idea and your target
   length.
3. Paste it into a chat assistant.
4. Save the reply as a plain `.txt` file. **Do not edit the reply** other than to
   delete anything that is not the script itself.

The prompt is written so the reply is *already* in the exact shape the pipeline
wants — no titles, no formatting, no commentary to strip out.

---

## The prompt

```text
You are writing narration for a short vertical video. The text you produce will
be read aloud by a text-to-speech engine and captioned word by word on screen.
Nothing else will be added — no host, no dialogue, no sound effects.

Write the script according to these rules.

LENGTH
Aim for roughly 2.3 spoken words per second. So:
  30 seconds  ~=  70 words
  45 seconds  ~= 105 words
  60 seconds  ~= 140 words
Count your words and stay within 10% of the target. Going long is worse than
going short.

LINE STRUCTURE — this one matters most
Write in short lines, one beat per line, with a blank line between each.
Each line should be a complete thought that can stand on screen by itself.
Most lines should be 4 to 12 words. Never write a paragraph.

This structure is not cosmetic: each line becomes a caption, and the pause
between lines becomes the rhythm of the video. Long lines produce cramped
captions and a breathless read.

CRAFT
- The first line is the hook. Start mid-situation, not with setup. No "Have you
  ever wondered". No throat-clearing.
- Use short, plain sentences. Concrete nouns. Active verbs.
- Escalate. Each line should raise the stakes or narrow the focus.
- Land a turn near the end — a reveal, a reversal, or a realisation.
- End on the strongest line. Do not explain the ending or add a moral.
- Do not address the viewer, ask them to like or subscribe, or mention the video
  itself.

VOICE
Write for the ear, not the eye. Read it aloud in your head. If a line is hard to
say in one breath, it is too long. Prefer rhythm over cleverness.

FORMAT — follow this exactly
- Output ONLY the script. No title, no heading, no preamble, no sign-off.
- Do not say "Here is your script" or anything before or after it.
- Plain text only. No markdown, no bold, no bullet points, no numbering.
- No emoji.
- No quotation marks around the whole script.
- No stage directions, no bracketed cues like [pause] or [whispers], and no
  narrator notes. The engine cannot act on them and they would be read aloud.
- Straight quotes only (") for any dialogue inside the story.
- Blank line between every line of narration.

Here is an example of the exact output format expected:

At exactly 2:17 every morning, Daniel's phone received a call.

The caller never spoke.

At first, he thought it was a prank.

Then one night, he answered.

And heard himself whispering from the other end.

"Don't open the door."

Daniel froze.

Because someone had just knocked.

Now write the script.

TARGET LENGTH: [45 seconds]
PLOT IDEA: [describe your idea here in a sentence or two]
```

---

## Notes

**Why no `[pause]` or `[whispers]` markers.** The current voice engine
(`edge-tts`) cannot act on them — it would read them aloud as words, and they
would appear in your captions. Pacing today comes from line structure and
punctuation, which is why the prompt leans on those so hard. When expressive
voice providers are added, this prompt gains a markup section.

**Why the word counts.** Measured, not estimated: two real renders came out at
2.31 words per second through `en-US-AriaNeural` at default rate. If you change
the voice or the `rate` setting, re-measure — a faster rate means more words fit.

**Line length drives captions.** A caption group closes on sentence-ending
punctuation, so a line ending in `.`, `!` or `?` becomes its own caption. This is
why one-beat-per-line produces clean captions and a run-on paragraph does not.
