from orenza.models import Enzyme, Sprot, Ec


def link_swiss_explorenz():
    ec = Ec.objects.all()
    for e in ec:
        if e.complete:
            if Enzyme.objects.filter(pk=e.number).exists():
                count = len(e.sprots.all())
                enzyme = Enzyme.objects.get(pk=e.number)
                if enzyme.orphan:
                    enzyme.orphan = False
                enzyme.sprot_count += count
                enzyme.save()
