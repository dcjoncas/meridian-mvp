Meridian loading + polling fix

What was wrong:
- the network modal close button listener was attaching before that element existed
- that could break the member page JavaScript and make the page appear stuck / empty

What is fixed:
- safe event binding for My Network modal
- page loads correctly again
- polling reduced from every 5 seconds to every 15 seconds
- polling now pauses while the tab is hidden

Why you kept seeing repeated GET calls:
- that was the auto-refresh polling in member.html
- inbox / outbox / profile summary were refreshing on an interval by design

Replace:
- member.html

You can keep the rest of your current working files.
