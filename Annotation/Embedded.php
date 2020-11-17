<?php

/*
 * This file is part of the ONGR package.
 *
 * (c) NFQ Technologies UAB <info@nfq.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace ONGR\ElasticsearchBundle\Annotation;

/**
 * @Annotation
 * @Target("PROPERTY")
 */
final class Embedded extends AbstractAnnotation implements PropertiesAwareInterface
{
    use NameAwareTrait;

    /**
     * Inner object class name.
     *
     * @var string Object name to map
     *
     * @Doctrine\Common\Annotations\Annotation\Required
     */
    public $class;

    /**
     * Defines if related value will store a single object or an array of objects
     *
     * If this value is set to true, in the result ObjectIterator will be provided,
     * otherwise you will get single object.
     *
     * @var bool Object or ObjectIterator
     */
    public $multiple;
}
